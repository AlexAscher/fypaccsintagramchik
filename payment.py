import requests
import os

CRYPTO_BOT_TOKEN = os.getenv("CRYPTO_BOT_TOKEN", "ТВОЙ_ТОКЕН_CRYPTO_BOT")
CRYPTO_API_URL = "https://testnet-pay.crypt.bot/api"  # Для тестнета

def create_invoice(asset: str, amount: float, description: str, payload: str):
    headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
    data = {
        "asset": asset,
        "amount": amount,
        "description": description,
        "hidden_message": "Спасибо за покупку!",
        "payload": payload
    }

    response = requests.post(f"{CRYPTO_API_URL}/createInvoice", json=data, headers=headers)
    response.raise_for_status()
    return response.json()["result"]
