import sys
import os

# Додаємо src у sys.path, щоб імпорти працювали
repo_root = os.path.abspath("mexc-api-sdk/src")
if repo_root not in sys.path:
    sys.path.append(repo_root)

from modules.contract import Contract

# Вкажи свої API ключі
API_KEY = "тут_твій_API_key"
API_SECRET = "тут_твій_API_secret"

def main():
    client = Contract(API_KEY, API_SECRET)
    result = client.pair_list()
    print(result)

if __name__ == "__main__":
    main()
