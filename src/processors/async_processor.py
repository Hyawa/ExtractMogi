"""
Módulo responsável pelo processamento assíncrono de empresas.
Integrado com Textual UI para feedback em tempo real.
Inclui tratamento de CAPTCHA e delays anti-detecção.
"""

import csv
import asyncio
import random
from pathlib import Path
from typing import List, Dict, Optional, Callable, Any
from datetime import datetime
import logging

from ..scrappers.google_scraper import GoogleScraper, CaptchaDetectedException
from ..scrappers.facebook_scraper import FacebookScraper

logger = logging.getLogger(__name__)


class AsyncCSVProcessor:
    """
    Processador assíncrono de CSV para integração com Textual UI.
    Fornece callbacks para atualização em tempo real da interface.
    """

    def __init__(
        self,
        csv_path: str,
        db_session,
        model_class,
        headless: bool = True,
        slow_mo: int = 2000,
    ):
        """
        Inicializa o processador assíncrono.

        Args:
            csv_path: Caminho para o arquivo CSV
            db_session: Sessão do SQLAlchemy
            model_class: Classe do modelo ExtractMogi
            headless: Se o browser deve rodar em modo headless
            slow_mo: Delay em ms entre ações do Playwright (anti-detecção)
        """
        self.csv_path = Path(csv_path)
        self.db_session = db_session
        self.model_class = model_class
        self.headless = headless
        self.slow_mo = slow_mo

        # Callbacks para UI
        self.on_progress = None
        self.on_company_start = None
        self.on_company_complete = None
        self.on_error = None
        self.on_captcha_detected = None

        # Controle de CAPTCHA
        self.captcha_mode = False

        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV não encontrado: {csv_path}")

    def set_callbacks(
        self,
        on_progress: Optional[Callable] = None,
        on_company_start: Optional[Callable] = None,
        on_company_complete: Optional[Callable] = None,
        on_error: Optional[Callable] = None,
        on_captcha_detected: Optional[Callable] = None,
    ):
        """
        Define callbacks para comunicação com a UI.

        Args:
            on_progress: Callback(current, total, percentage)
            on_company_start: Callback(nome_empresa)
            on_company_complete: Callback(nome_empresa, data)
            on_error: Callback(nome_empresa, error_message)
            on_captcha_detected: Callback(nome_empresa, message)
        """
        self.on_progress = on_progress
        self.on_company_start = on_company_start
        self.on_company_complete = on_company_complete
        self.on_error = on_error
        self.on_captcha_detected = on_captcha_detected

    def read_csv(self) -> List[str]:
        """
        Lê o CSV e retorna lista de nomes de empresas.

        Returns:
            Lista com nomes das empresas
        """
        companies = []

        try:
            with open(self.csv_path, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)

                for row in reader:
                    nome = row.get("Nome_Fantasia", "").strip()
                    if nome:
                        companies.append(nome)

            logger.info(f"CSV lido com sucesso: {len(companies)} empresas encontradas")

        except Exception as e:
            logger.error(f"Erro ao ler CSV: {str(e)}")
            raise

        return companies

    async def process_all(self) -> Dict[str, int]:
        """
        Processa todas as empresas do CSV de forma assíncrona.

        Returns:
            Dict com estatísticas do processamento
        """
        companies = self.read_csv()
        total = len(companies)

        logger.info(f"Iniciando processamento assíncrono de {total} empresas")

        stats = {
            "total": total,
            "processadas": 0,
            "com_dados": 0,
            "sem_dados": 0,
            "erros": 0,
            "captchas": 0,
        }

        async with GoogleScraper(
            headless=self.headless, slow_mo=self.slow_mo
        ) as google_scraper:

            # Define callback de CAPTCHA no scraper
            google_scraper.set_captcha_callback(self._on_captcha_callback)

            for idx, nome_empresa in enumerate(companies, 1):
                try:
                    # Notifica início do processamento
                    if self.on_company_start:
                        await self.on_company_start(nome_empresa)

                    # Atualiza progresso
                    if self.on_progress:
                        percentage = int((idx / total) * 100)
                        await self.on_progress(idx, total, percentage)

                    logger.info(f"[{idx}/{total}] Processando: {nome_empresa}")

                    # Processa a empresa
                    company_data = await self._process_company(
                        google_scraper, nome_empresa
                    )

                    # Salva no banco
                    self._save_to_database(company_data)

                    # Atualiza estatísticas
                    stats["processadas"] += 1

                    # Verifica se encontrou algum dado
                    has_data = any(
                        [
                            company_data["telefone"],
                            company_data["site"],
                            company_data["facebook_link"],
                            company_data["email"],
                            company_data["celular_whatsapp"],
                        ]
                    )

                    if has_data:
                        stats["com_dados"] += 1
                    else:
                        stats["sem_dados"] += 1

                    # Notifica conclusão
                    if self.on_company_complete:
                        await self.on_company_complete(nome_empresa, company_data)

                    # DELAY ALEATÓRIO entre empresas (evita CAPTCHA)
                    delay = random.uniform(3, 7)
                    logger.debug(f"Aguardando {delay:.2f}s antes da próxima empresa...")
                    await asyncio.sleep(delay)

                except CaptchaDetectedException as e:
                    error_msg = str(e)
                    logger.error(f"CAPTCHA detectado para {nome_empresa}: {error_msg}")
                    stats["captchas"] += 1

                    # Notifica erro de CAPTCHA
                    if self.on_error:
                        await self.on_error(nome_empresa, f"CAPTCHA: {error_msg}")

                    # Se estiver em headless, sugere modo visual
                    if self.headless:
                        logger.warning(
                            "DICA: Execute novamente com headless=False para "
                            "resolver CAPTCHAs manualmente"
                        )
                        # Pode optar por parar ou continuar
                        # Por padrão, vamos continuar com as próximas empresas
                        continue

                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Erro ao processar {nome_empresa}: {error_msg}")
                    stats["erros"] += 1

                    # Notifica erro
                    if self.on_error:
                        await self.on_error(nome_empresa, error_msg)

        logger.info(f"Processamento concluído: {stats}")
        return stats

    async def _on_captcha_callback(self, nome_empresa: str):
        """
        Callback interno chamado quando CAPTCHA é detectado.

        Args:
            nome_empresa: Nome da empresa
        """
        logger.warning(f"CAPTCHA detectado para: {nome_empresa}")

        if self.on_captcha_detected:
            await self.on_captcha_detected(
                nome_empresa, "CAPTCHA detectado! Aguardando resolução manual..."
            )

    async def _process_company(
        self, google_scraper: GoogleScraper, nome_empresa: str
    ) -> Dict[str, Optional[str]]:
        """
        Processa uma única empresa.

        Args:
            google_scraper: Instância do GoogleScraper
            nome_empresa: Nome da empresa

        Returns:
            Dict com todos os dados extraídos
        """
        # Busca dados no Google
        google_data = await google_scraper.search_company(nome_empresa)

        # Dados completos
        company_data = {
            "nome_empresa": nome_empresa,
            "telefone": google_data.get("telefone"),
            "site": google_data.get("site"),
            "facebook_link": google_data.get("facebook_link"),
            "email": None,
            "celular_whatsapp": None,
        }

        # Se encontrou Facebook, busca dados adicionais
        if google_data.get("facebook_link"):
            try:
                facebook_data = await self._extract_facebook_data(
                    google_scraper.context, google_data["facebook_link"]
                )
                company_data.update(facebook_data)
            except Exception as e:
                logger.error(f"Erro ao extrair dados do Facebook: {e}")

        return company_data

    async def _extract_facebook_data(
        self, context, facebook_url: str
    ) -> Dict[str, Optional[str]]:
        """
        Extrai dados do Facebook.

        Args:
            context: Contexto do browser do Playwright
            facebook_url: URL da página do Facebook

        Returns:
            Dict com email e celular_whatsapp
        """
        page = await context.new_page()

        try:
            facebook_data = await FacebookScraper.extract_contact_info(
                page, facebook_url
            )
            return facebook_data
        finally:
            await page.close()

    def _save_to_database(self, data: Dict):
        """
        Salva dados no banco de dados.

        Args:
            data: Dados da empresa para salvar
        """
        try:
            # Verifica se a empresa já existe
            existing = (
                self.db_session.query(self.model_class)
                .filter_by(nome_empresa=data["nome_empresa"])
                .first()
            )

            if existing:
                # Atualiza registro existente
                for key, value in data.items():
                    if key != "nome_empresa" and value is not None:
                        setattr(existing, key, value)
                logger.info(f"Registro atualizado: {data['nome_empresa']}")
            else:
                # Cria novo registro
                new_record = self.model_class(**data)
                self.db_session.add(new_record)
                logger.info(f"Novo registro criado: {data['nome_empresa']}")

            self.db_session.commit()

        except Exception as e:
            logger.error(f"Erro ao salvar no banco: {str(e)}")
            self.db_session.rollback()
            raise
