@echo off
echo ================================================
echo     MENJALANKAN EXCEL CONVERTER
echo ================================================
echo.

:: Cek apakah virtual environment sudah dibuat
if not exist venv (
    echo [ERROR] Virtual environment tidak ditemukan.
    echo         Silakan jalankan install.bat terlebih dahulu.
    pause
    exit /b 1
)

:: Buat folder temp jika belum ada
if not exist temp (
    mkdir temp
)

:: Buat folder df_transformation jika belum ada
if not exist df_transformation (
    mkdir df_transformation
    echo # placeholder > df_transformation\__init__.py
)

:: Aktifkan virtual environment dan jalankan aplikasi
echo [INFO] Menjalankan aplikasi...
call venv\Scripts\activate

:: Jalankan Streamlit
streamlit run main.py
if %errorlevel% neq 0 (
    echo [ERROR] Gagal menjalankan aplikasi.
    echo [INFO] Mencoba metode alternatif...
    python -m streamlit run main.py
    if %errorlevel% neq 0 (
        echo [ERROR] Metode alternatif juga gagal.
        echo [TIP] Pastikan instalasi berhasil dengan menjalankan install.bat kembali.
        pause
    )
)

:: Deaktifkan virtual environment saat aplikasi ditutup
call venv\Scripts\deactivate

exit /b 0
