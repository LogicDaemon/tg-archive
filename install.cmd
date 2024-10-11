@(
    IF NOT EXIST "%~dp0.venv\Scripts\activate.bat" (
        py -m venv "%~dp0.venv"
    )
    CALL "%~dp0.venv\Scripts\activate.bat"
    python -m pip install --require-virtualenv --edit .
)
