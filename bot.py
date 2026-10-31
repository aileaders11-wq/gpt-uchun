import asyncio
from html import escape
import io
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

import aiohttp
from aiohttp import web
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import FSInputFile, Message

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MAX_USERNAME_COUNT = 1000
CONCURRENT_REQUESTS = 20
PROGRESS_UPDATE_SECONDS = 3.5
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=12, connect=5, sock_connect=5, sock_read=10)
HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/130.0 Safari/537.36", "Accept-Language": "en-US,en;q=0.9"}
USERNAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9]{4,31}$")
router = Router()
user_locks: dict[int, asyncio.Lock] = {}


@dataclass
class Result:
    username: str
    status: str
    error: Optional[str] = None


def extract_usernames(text: str) -> tuple[list[str], list[str]]:
    valid, invalid, seen = [], [], set()
    for item in re.split(r"[,;\s]+", text):
        item = item.strip().lstrip("@")
        if not item:
            continue
        if USERNAME_PATTERN.fullmatch(item):
            item = item.lower()
            if item not in seen:
                valid.append(item)
                seen.add(item)
        else:
            invalid.append(item)
    return valid, invalid


async def read_txt(bot: Bot, message: Message) -> str:
    document = message.document
    if not document or not (document.file_name or "").lower().endswith(".txt"):
        raise ValueError("Faqat .txt fayl yuboring.")
    if document.file_size and document.file_size > 5 * 1024 * 1024:
        raise ValueError("Fayl hajmi 5 MB dan katta bo‘lmasin.")
    buffer = io.BytesIO()
    await bot.download(document, destination=buffer)
    return buffer.getvalue().decode("utf-8-sig", errors="ignore")


async def request_html(session: aiohttp.ClientSession, url: str) -> tuple[int, str]:
    for attempt in range(3):
        try:
            async with session.get(url, headers=HEADERS) as response:
                text = await response.text(errors="ignore")
                if response.status == 429 or response.status >= 500:
                    if attempt < 2:
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue
                return response.status, text
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == 2:
                raise
            await asyncio.sleep(1.2 * (attempt + 1))
    return 0, ""


def telegram_is_free(username: str, html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    expected_title = f"Telegram: Contact @{username}"
    has_profile = bool(soup.select_one(".tgme_page_title, .tgme_page_extra"))
    return title.casefold() == expected_title.casefold() and not has_profile


async def check_one(username: str, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> Result:
    async with semaphore:
        try:
            # Render serverida t.me ayrim hollarda proxy sahifasini qaytaradi.
            # telegram.me esa xuddi shu Telegram sahifasining ishlaydigan aliasidir.
            code, html = await request_html(session, f"https://telegram.me/{quote(username)}")
            if code != 200:
                return Result(username, "BAND")
            return Result(
                username,
                "BO‘SH" if telegram_is_free(username, html) else "BAND",
            )
        except asyncio.TimeoutError:
            return Result(username, "BAND")
        except Exception:
            logging.exception("Check failed for %s", username)
            return Result(username, "BAND")


async def check_all(usernames: list[str], progress: Message) -> list[Result]:
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS, limit_per_host=CONCURRENT_REQUESTS, ttl_dns_cache=300)
    done = 0
    stop = asyncio.Event()
    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT, connector=connector, headers=HEADERS) as session:
        tasks = [asyncio.create_task(check_one(u, session, semaphore)) for u in usernames]

        async def progress_loop():
            while not stop.is_set():
                try:
                    await progress.edit_text(f"⏳ Tekshirilmoqda: {done}/{len(usernames)}...")
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(stop.wait(), timeout=PROGRESS_UPDATE_SECONDS)
                except asyncio.TimeoutError:
                    pass

        progress_task = asyncio.create_task(progress_loop())
        results = []
        try:
            for task in asyncio.as_completed(tasks):
                results.append(await task)
                done += 1
        finally:
            stop.set()
            await progress_task
        result_map = {r.username: r for r in results}
        return [result_map[u] for u in usernames]


def statistics(results: list[Result]) -> str:
    busy_count = sum(r.status == "BAND" for r in results)
    free_count = sum(r.status == "BO‘SH" for r in results)
    free = [r.username for r in results if r.status == "BO‘SH"]

    text = (
        "<b>✅ Tekshiruv tugadi!</b>\n\n"
        f"<b>📊 Jami:</b> {len(results)}\n"
        f"<b>🔴 Band:</b> {busy_count}\n"
        f"<b>🟢 Bo‘sh:</b> {free_count}"
    )

    if free and len(free) <= 80:
        text += (
            "\n\n<b>🟢 Bo‘sh username’lar:</b>\n"
            + "\n".join(f"@{escape(username)}" for username in free)
        )
    elif not free:
        text += "\n\n<b>🟢 Bo‘sh username topilmadi.</b>"

    return text


async def send_free_file(message: Message, usernames: list[str]):
    path = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".txt", delete=False) as file:
            path = file.name
            file.write("\n".join(f"@{u}" for u in usernames))
        await message.answer_document(FSInputFile(path, filename="bosh_userlar.txt"), caption=f"🟢 Bo‘sh username'lar: {len(usernames)} ta")
    finally:
        if path and os.path.exists(path):
            os.remove(path)


async def process(message: Message, raw_text: str):
    lock = user_locks.setdefault(message.chat.id, asyncio.Lock())
    if lock.locked():
        await message.answer("⏳ Sizning oldingi tekshiruvingiz hali tugamadi.")
        return
    async with lock:
        usernames, invalid = extract_usernames(raw_text)
        if not usernames:
            await message.answer("❌ Yaroqli username topilmadi.")
            return
        if len(usernames) > MAX_USERNAME_COUNT:
            await message.answer(f"❌ Maksimal limit: {MAX_USERNAME_COUNT} ta username.")
            return
        progress = await message.answer(f"⏳ Tekshirilmoqda: 0/{len(usernames)}...")
        results = await check_all(usernames, progress)
        await progress.edit_text(f"✅ Tekshiruv tugadi: {len(results)}/{len(usernames)}")
        await message.answer(statistics(results))
        free = [r.username for r in results if r.status == "BO‘SH"]
        if len(free) > 80:
            await send_free_file(message, free)
        if invalid:
            invalid_text = ", ".join(escape(item) for item in invalid[:30])
            await message.answer(
                "<b>⚠️ Qoidaga mos kelmaganlar:</b>\n" + invalid_text
            )


@router.message(CommandStart())
async def start(message: Message):
    await message.answer("Username'larni vergul, probel, yangi qator yoki .txt fayl ko‘rinishida yuboring.")


@router.message(lambda message: bool(message.document))
async def document_handler(message: Message, bot: Bot):
    try:
        await process(message, await read_txt(bot, message))
    except ValueError as exc:
        await message.answer(f"❌ {exc}")


@router.message()
async def text_handler(message: Message):
    if message.text:
        await process(message, message.text)


async def health(request):
    return web.Response(text="OK")


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable topilmadi")
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "10000")))
    await site.start()
    try:
        await dp.start_polling(bot)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
