import requests
import os

CRYPTOBOT_API_TOKEN = os.environ["CRYPTOBOT_API_TOKEN"]
CRYPTO_API_URL = "https://testnet-pay.crypt.bot/api"  # Для тестнета

def create_invoice(asset: str, amount: float, description: str, payload: str):
    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
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
