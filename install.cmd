@(
    IF EXIST "%LOCALAPPDATA%\Temp_" (
        SET "TEMP=%LOCALAPPDATA%\Temp_"
        SET "TMP=%LOCALAPPDATA%\Temp_"
    )
    python.exe -m venv "%~dp0.venv"
    CALL "%~dp0.venv\Scripts\activate.bat"
    python -m pip install -U --require-virtualenv --edit .
)
