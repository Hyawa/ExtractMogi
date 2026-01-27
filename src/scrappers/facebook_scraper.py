"""
Módulo responsável pela extração de dados do Facebook.
"""

import re
import asyncio
from typing import Optional, Dict
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout
import logging

logger = logging.getLogger(__name__)


class FacebookScraper:
    """Scraper para extrair informações de contato do Facebook."""

    @staticmethod
    async def extract_contact_info(
        page: Page, facebook_url: str
    ) -> Dict[str, Optional[str]]:
        """
        Extrai informações de contato de uma página do Facebook.

        Args:
            page: Página do Playwright
            facebook_url: URL da página do Facebook

        Returns:
            Dict com email e celular_whatsapp
        """
        result = {"email": None, "celular_whatsapp": None}

        try:
            logger.info(f"Acessando Facebook: {facebook_url}")

            # Acessa a página do Facebook
            await page.goto(facebook_url, wait_until="domcontentloaded")
            await asyncio.sleep(2)  # Aguarda carregamento dinâmico

            # Tenta navegar para a seção "Sobre" ou "About"
            await FacebookScraper._navigate_to_about(page)

            # Aguarda o carregamento da página
            await asyncio.sleep(2)

            # Extrai o conteúdo da página
            content = await page.content()

            # Extrai email
            result["email"] = FacebookScraper._extract_email(content)

            # Extrai WhatsApp/Celular
            result["celular_whatsapp"] = FacebookScraper._extract_whatsapp(content)

            logger.info(f"Dados do Facebook extraídos: {result}")

        except PlaywrightTimeout:
            logger.warning(f"Timeout ao acessar Facebook: {facebook_url}")
        except Exception as e:
            logger.error(f"Erro ao extrair dados do Facebook: {str(e)}")

        return result

    @staticmethod
    async def _navigate_to_about(page: Page):
        """Tenta navegar para a seção Sobre/About da página."""
        try:
            # Tenta clicar no link "Sobre" ou "About"
            about_selectors = [
                'a[href*="/about"]',
                'a:has-text("Sobre")',
                'a:has-text("About")',
                'a:has-text("Informações")',
                'a:has-text("Info")',
            ]

            for selector in about_selectors:
                try:
                    about_link = await page.query_selector(selector)
                    if about_link:
                        await about_link.click()
                        await page.wait_for_load_state("domcontentloaded")
                        logger.info("Navegou para seção Sobre")
                        return
                except:
                    continue

        except Exception as e:
            logger.debug(f"Não foi possível navegar para seção Sobre: {str(e)}")

    @staticmethod
    def _extract_email(content: str) -> Optional[str]:
        """
        Extrai email do conteúdo HTML.

        Args:
            content: HTML da página

        Returns:
            Email encontrado ou None
        """
        # Padrão de email
        email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"

        # Remove emails comuns de redes sociais que não são da empresa
        excluded_domains = [
            "facebook.com",
            "instagram.com",
            "twitter.com",
            "google.com",
            "outlook.com",
            "example.com",
        ]

        emails = re.findall(email_pattern, content)

        for email in emails:
            # Verifica se não é de domínio excluído
            if not any(domain in email.lower() for domain in excluded_domains):
                logger.info(f"Email encontrado: {email}")
                return email.lower()

        return None

    @staticmethod
    def _extract_whatsapp(content: str) -> Optional[str]:
        """
        Extrai número de WhatsApp/Celular do conteúdo HTML.
        Procura por números com DDD 19 (Mogi Mirim) e 9 dígitos.

        Args:
            content: HTML da página

        Returns:
            Número formatado ou None
        """
        # Padrões de telefone/WhatsApp com DDD 19
        phone_patterns = [
            r"\(19\)\s*9\s*\d{4}[-\s]?\d{4}",  # (19) 9 1234-5678
            r"19\s*9\s*\d{4}[-\s]?\d{4}",  # 19 9 1234-5678
            r"\+55\s*19\s*9\s*\d{4}[-\s]?\d{4}",  # +55 19 9 1234-5678
            r"55\s*19\s*9\s*\d{4}[-\s]?\d{4}",  # 55 19 9 1234-5678
        ]

        for pattern in phone_patterns:
            matches = re.findall(pattern, content)
            if matches:
                # Pega o primeiro match e limpa
                phone = re.sub(r"[^\d]", "", matches[0])

                # Verifica se é um celular válido (DDD 19 + 9 dígitos)
                if len(phone) >= 11 and phone[-11:-9] == "19" and phone[-10] == "9":
                    formatted = FacebookScraper._format_whatsapp(phone[-11:])
                    logger.info(f"WhatsApp encontrado: {formatted}")
                    return formatted

        # Tenta padrão mais genérico de 9 dígitos após encontrar contexto de WhatsApp
        whatsapp_context = (
            r"(?i)(whatsapp|celular|contato|telefone)[\s\S]{0,50}9\s*\d{4}[-\s]?\d{4}"
        )
        context_matches = re.findall(whatsapp_context, content)

        if context_matches:
            # Extrai apenas o número
            number_match = re.search(r"9\s*\d{4}[-\s]?\d{4}", context_matches[0])
            if number_match:
                phone = re.sub(r"[^\d]", "", number_match.group())
                if len(phone) == 9:
                    formatted = f"(19) {FacebookScraper._format_whatsapp(f'19{phone}')}"
                    logger.info(f"WhatsApp encontrado (contexto): {formatted}")
                    return formatted

        return None

    @staticmethod
    def _format_whatsapp(phone: str) -> str:
        """
        Formata número de WhatsApp.

        Args:
            phone: String com apenas números

        Returns:
            Número formatado
        """
        # Remove tudo que não é número
        phone = re.sub(r"[^\d]", "", phone)

        # Formato: (19) 99999-9999
        if len(phone) >= 11:
            # Pega os últimos 11 dígitos
            phone = phone[-11:]
            return f"({phone[:2]}) {phone[2:7]}-{phone[7:]}"
        elif len(phone) == 9:
            return f"(19) {phone[:5]}-{phone[5:]}"

        return phone
