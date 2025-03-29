@echo off
echo ================================================
echo     UNINSTALL EXCEL CONVERTER
echo ================================================
echo.

echo [INFO] Menghapus virtual environment...

:: Cek apakah virtual environment ada
if not exist venv (
    echo [INFO] Virtual environment tidak ditemukan. Tidak ada yang perlu dihapus.
) else (
    :: Hapus folder virtual environment
    rmdir /s /q venv
    if %errorlevel% neq 0 (
        echo [ERROR] Gagal menghapus virtual environment.
        echo [TIP] Tutup semua aplikasi yang mungkin menggunakan file di folder venv.
        pause
        exit /b 1
    ) else (
        echo [INFO] Virtual environment berhasil dihapus.
    )
)

:: Hapus file temporary jika ada
if exist temp (
    echo [INFO] Menghapus file temporary...
    rmdir /s /q temp
    if %errorlevel% neq 0 (
        echo [WARNING] Beberapa file temporary tidak dapat dihapus.
    ) else {
        echo [INFO] File temporary berhasil dihapus.
    }
)

echo.
echo ================================================
echo      UNINSTALL SELESAI
echo ================================================
echo.
echo Excel Converter berhasil diuninstall.
echo.
pause
