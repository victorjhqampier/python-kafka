# Copy-Item -Force "requirements.txt" "src/requirements.txt"
Copy-Item -Force ".env" "src/.env"

Set-Location src

python -m venv env

Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope CurrentUser

.\env\Scripts\Activate.ps1

python -m pip install --upgrade pip

pip install -r requirements.txt

uvicorn Interface.app:app --reload