"""
Módulo responsável pela extração de dados do Google Meu Negócio.
Inclui detecção e resolução de CAPTCHA com intervenção humana.
"""

import re
import asyncio
import random
from urllib.parse import urlparse, parse_qs
from typing import Optional, Dict, Callable
from playwright.async_api import (
    async_playwright,
    Page,
    TimeoutError as PlaywrightTimeout,
)
import logging

logger = logging.getLogger(__name__)


class CaptchaDetectedException(Exception):
    """Exceção levantada quando um CAPTCHA é detectado."""

    pass


class GoogleScraper:
    """Scraper para extrair informações do Google Meu Negócio com detecção de CAPTCHA."""

    def __init__(self, headless: bool = True, slow_mo: int = 2000):
        """
        Inicializa o Google Scraper.

        Args:
            headless: Se True, executa em modo headless (sem interface gráfica)
            slow_mo: Delay em ms entre ações (ajuda a evitar CAPTCHA)
        """
        self.headless = headless
        self.slow_mo = slow_mo
        self.browser = None
        self.context = None
        self.captcha_callback = None
        self._captcha_detected = False

    def set_captcha_callback(self, callback: Callable):
        """
        Define callback para notificar a UI quando CAPTCHA é detectado.

        Args:
            callback: Função a ser chamada quando CAPTCHA for detectado
        """
        self.captcha_callback = callback

    async def __aenter__(self):
        """Inicializa o browser ao entrar no contexto."""
        self.playwright = await async_playwright().start()

        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            slow_mo=self.slow_mo,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )

        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
        )

        await self.context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """
        )

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Fecha o browser ao sair do contexto."""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def search_company(self, nome_empresa: str) -> Dict[str, Optional[str]]:
        """
        Pesquisa uma empresa no Google e extrai informações.

        Args:
            nome_empresa: Nome fantasia da empresa

        Returns:
            Dict com telefone, facebook_link e site

        Raises:
            CaptchaDetectedException: Se um CAPTCHA for detectado
        """
        result = {"telefone": None, "facebook_link": None, "site": None}

        page = await self.context.new_page()

        try:
            search_query = f'"{nome_empresa}" Mogi Mirim'
            logger.info(f"Buscando: {search_query}")

            await page.set_extra_http_headers(
                {
                    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                }
            )

            await page.goto(
                f"https://www.google.com/search?q={search_query}",
                wait_until="domcontentloaded",
            )

            await asyncio.sleep(random.uniform(1, 2))

            captcha_detected = await self._check_for_captcha(page)

            if captcha_detected:
                self._captcha_detected = True

                if self.captcha_callback:
                    await self.captcha_callback(nome_empresa)

                if self.headless:
                    logger.warning(
                        "CAPTCHA detectado em modo headless. Sugerindo modo visual."
                    )
                    raise CaptchaDetectedException(
                        "CAPTCHA detectado. Execute com headless=False para resolver manualmente."
                    )

                logger.warning("CAPTCHA detectado! Aguardando intervenção humana...")
                await self._wait_for_human_intervention(page, nome_empresa)
                self._captcha_detected = False

            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)
            await page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(1)

            result["telefone"] = await self._extract_phone(page)
            result["site"] = await self._extract_website(page)
            result["facebook_link"] = await self._extract_facebook_link(page)

            logger.info(f"Dados extraídos para {nome_empresa}: {result}")

        except CaptchaDetectedException:
            raise
        except Exception as e:
            logger.error(f"Erro ao buscar {nome_empresa}: {str(e)}")
        finally:
            await page.close()

        return result

    async def _check_for_captcha(self, page: Page) -> bool:
        """
        Verifica se um CAPTCHA foi detectado na página.

        Args:
            page: Página do Playwright

        Returns:
            True se CAPTCHA foi detectado, False caso contrário
        """
        try:
            captcha_indicators = [
                "form#captcha-form",
                "#captcha-form",
                "div.g-recaptcha",
                'iframe[src*="recaptcha"]',
            ]

            for selector in captcha_indicators:
                captcha_element = await page.query_selector(selector)
                if captcha_element:
                    logger.warning(f"CAPTCHA detectado via seletor: {selector}")
                    return True

            content = await page.content()
            captcha_texts = [
                "unusual traffic",
                "tráfego incomum",
                "sistemas automatizados",
                "automated queries",
                "Our systems have detected unusual traffic",
            ]

            for text in captcha_texts:
                if text.lower() in content.lower():
                    logger.warning(f"CAPTCHA detectado via texto: {text}")
                    return True

            return False

        except Exception as e:
            logger.debug(f"Erro ao verificar CAPTCHA: {e}")
            return False

    async def _wait_for_human_intervention(self, page: Page, nome_empresa: str):
        """
        Aguarda intervenção humana para resolver o CAPTCHA.

        Args:
            page: Página do Playwright
            nome_empresa: Nome da empresa sendo processada
        """
        logger.warning("=" * 60)
        logger.warning("⚠️  CAPTCHA DETECTADO - INTERVENÇÃO NECESSÁRIA")
        logger.warning("=" * 60)
        logger.warning(f"Empresa: {nome_empresa}")
        logger.warning("Instruções:")
        logger.warning("1. Resolva o CAPTCHA manualmente na janela do navegador")
        logger.warning("2. Aguarde até ver os resultados da busca aparecerem")
        logger.warning("3. O script continuará automaticamente")
        logger.warning("=" * 60)

        try:
            await page.wait_for_selector("div#search", timeout=300000)

            await asyncio.sleep(2)

            logger.info("✓ Resultados de busca detectados. Continuando...")

        except PlaywrightTimeout:
            logger.error("Timeout aguardando resolução do CAPTCHA")
            raise CaptchaDetectedException(
                "Timeout ao aguardar resolução manual do CAPTCHA"
            )

    async def _extract_phone(self, page: Page) -> Optional[str]:
        """Extrai o número de telefone do widget do Google."""
        try:
            phone_button = await page.query_selector('[aria-label*="Ligar"]')
            if phone_button:
                aria_label = await phone_button.get_attribute("aria-label")
                phone = re.sub(r"[^\d]", "", aria_label)
                if phone:
                    return self._format_phone(phone)

            phone_patterns = [
                r"\(19\)\s*\d{4,5}[-\s]?\d{4}",
                r"19\s*\d{4,5}[-\s]?\d{4}",
                r"\d{4,5}[-\s]?\d{4}",
            ]

            content = await page.content()
            for pattern in phone_patterns:
                match = re.search(pattern, content)
                if match:
                    phone = re.sub(r"[^\d]", "", match.group())
                    return self._format_phone(phone)

        except Exception as e:
            logger.debug(f"Erro ao extrair telefone: {str(e)}")

        return None

    async def _extract_website(self, page: Page) -> Optional[str]:
        """Extrai o link do site oficial do widget do Google."""
        try:
            selectors = [
                '[data-attrid="kc:/local:all in one"] a[href*="http"]',
                'a[data-dtype="d3ifr"]',
                'a[ping*="website"]',
            ]

            for selector in selectors:
                website_link = await page.query_selector(selector)
                if website_link:
                    href = await website_link.get_attribute("href")
                    if (
                        href
                        and "google.com" not in href
                        and "facebook.com" not in href
                        and "instagram.com" not in href
                    ):
                        if href.startswith("/url?"):
                            parsed = parse_qs(urlparse(href).query)
                            href = parsed.get("url", [parsed.get("q", [None])[0]])[0]
                        if href:
                            return href

            all_links = await page.query_selector_all("a[href*='http']")
            for link in all_links:
                href = await link.get_attribute("href")
                if not href:
                    continue

                if href.startswith("/url?"):
                    parsed = parse_qs(urlparse(href).query)
                    href = parsed.get("url", [parsed.get("q", [None])[0]])[0]

                if href and all(
                    domain not in href
                    for domain in [
                        "google.com",
                        "facebook.com",
                        "instagram.com",
                        "youtube.com",
                        "twitter.com",
                        "gstatic.com",
                        "googleapis.com",
                    ]
                ):
                    parent_text = ""
                    try:
                        parent = await link.evaluate_handle("el => el.parentElement")
                        parent_text = await parent.inner_text()
                    except:
                        pass

                    if any(
                        keyword in parent_text.lower()
                        for keyword in ["site", "website", "página", "visitar"]
                    ):
                        return href

        except Exception as e:
            logger.debug(f"Erro ao extrair website: {str(e)}")

        return None

    async def _extract_facebook_link(self, page: Page) -> Optional[str]:
        """Extrai o link do Facebook/Instagram independentemente do layout."""
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)

            all_links = await page.query_selector_all("a[href]")

            for link in all_links:
                href = await link.get_attribute("href")
                if not href:
                    continue

                if href.startswith("/url?"):
                    parsed = parse_qs(urlparse(href).query)
                    href = parsed.get("url", [parsed.get("q", [None])[0]])[0]

                if href and ("facebook.com" in href or "instagram.com" in href):
                    clean_url = href.split("?")[0].split("#")[0]
                    if "/posts/" not in clean_url and "/p/" not in clean_url:
                        return clean_url

        except Exception as e:
            logger.debug(f"Erro ao extrair Facebook/Instagram: {e}")

        return None

    @staticmethod
    def _format_phone(phone: str) -> str:
        """
        Formata o número de telefone.

        Args:
            phone: String com apenas números

        Returns:
            Telefone formatado
        """
        phone = re.sub(r"[^\d]", "", phone)

        if len(phone) == 11:
            return f"({phone[:2]}) {phone[2:7]}-{phone[7:]}"
        elif len(phone) == 10:
            return f"({phone[:2]}) {phone[2:6]}-{phone[6:]}"
        elif len(phone) == 9:
            return f"{phone[:5]}-{phone[5:]}"
        elif len(phone) == 8:
            return f"{phone[:4]}-{phone[4:]}"

        return phone
