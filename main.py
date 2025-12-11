import asyncio
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

BOT_TOKEN = "REDACTED_REVOKED_TOKEN"

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

router = Router()

def get_main_menu():
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton(text="Gmails + recovery", callback_data="category:Gmails"),
        InlineKeyboardButton(text="Warmed up IG accounts", callback_data="category:IG"),
        InlineKeyboardButton(text="Reddit warmed up accs", callback_data="category:Reddit"),
        InlineKeyboardButton(text="Tinder accs", callback_data="category:Tinder"),
        InlineKeyboardButton(text="Warmed up TikTok accounts", callback_data="category:TikTok"),
        InlineKeyboardButton(text="X accounts", callback_data="category:X"),
    )
    return kb

async def cmd_start(message: Message):
    await message.answer("Выберите категорию товара:", reply_markup=get_main_menu())

async def category_selected(callback: CallbackQuery):
    category = callback.data.split(":")[1]

    types_kb = InlineKeyboardMarkup(row_width=1)
    types = ["new", "brute", "old_3_5m", "old_1y"]
    for t in types:
        types_kb.add(
            InlineKeyboardButton(text=t.capitalize(), callback_data=f"type:{category}:{t}")
        )
    await callback.message.edit_text(f"Вы выбрали: {category}. Выберите тип аккаунта:", reply_markup=types_kb)
    await callback.answer()

async def type_selected(callback: CallbackQuery):
    _, category, acc_type = callback.data.split(":")
    price_usd = 7.0
    asset = "USDT"
    description = f"{category} — {acc_type}"
    invoice_url = f"https://example.com/pay?asset={asset}&amount={price_usd}&desc={description}"
    pay_kb = InlineKeyboardMarkup(row_width=1)
    pay_kb.add(
        InlineKeyboardButton(text="💸 Оплатить", url=invoice_url)
    )
    await callback.message.edit_text(
        f"🧾 Ваш заказ:\n{description}\n💰 Сумма: {price_usd} {asset}\n\n👇 Нажмите кнопку ниже, чтобы оплатить:",
        reply_markup=pay_kb
    )
    await callback.answer()

# Регистрируем обработчики
router.message.register(cmd_start, F.text == "/start")
router.callback_query.register(category_selected, F.data.startswith("category:"))
router.callback_query.register(type_selected, F.data.startswith("type:"))

dp.include_router(router)

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
