"""
Módulo responsável pelo processamento do CSV e coordenação da extração.
"""

import csv
import asyncio
from pathlib import Path
from typing import List, Dict, Optional, Callable
from datetime import datetime
import logging

from src.scrappers.google_scraper import GoogleScraper
from src.scrappers.facebook_scraper import FacebookScraper

logger = logging.getLogger(__name__)


class CSVProcessor:
    """Processa o CSV e coordena a extração de dados."""

    def __init__(
        self,
        csv_path: str,
        db_session,
        model_class,
        progress_callback: Optional[Callable] = None,
        headless: bool = True,
    ):
        """
        Inicializa o processador.

        Args:
            csv_path: Caminho para o arquivo CSV
            db_session: Sessão do SQLAlchemy
            model_class: Classe do modelo ExtractMogi
            progress_callback: Função opcional para reportar progresso
            headless: Se o browser deve rodar em modo headless
        """
        self.csv_path = Path(csv_path)
        self.db_session = db_session
        self.model_class = model_class
        self.progress_callback = progress_callback
        self.headless = headless

        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV não encontrado: {csv_path}")

    async def process(self) -> Dict[str, int]:
        """
        Processa todas as empresas do CSV.

        Returns:
            Dict com estatísticas do processamento
        """
        companies = self._read_csv()
        total = len(companies)

        logger.info(f"Iniciando processamento de {total} empresas")

        stats = {
            "total": total,
            "processadas": 0,
            "com_dados": 0,
            "sem_dados": 0,
            "erros": 0,
        }

        async with GoogleScraper(headless=self.headless) as google_scraper:
            for idx, nome_empresa in enumerate(companies, 1):
                try:
                    # Reporta progresso
                    if self.progress_callback:
                        self.progress_callback(idx, total, nome_empresa)

                    logger.info(f"[{idx}/{total}] Processando: {nome_empresa}")

                    # Busca dados no Google
                    google_data = await google_scraper.search_company(nome_empresa)

                    # Dados completos para salvar
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
                        facebook_data = await self._extract_facebook_data(
                            google_scraper.context, google_data["facebook_link"]
                        )
                        company_data.update(facebook_data)

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

                    # Pequeno delay entre requisições
                    await asyncio.sleep(1)

                except Exception as e:
                    logger.error(f"Erro ao processar {nome_empresa}: {str(e)}")
                    stats["erros"] += 1

        logger.info(f"Processamento concluído: {stats}")
        return stats

    def _read_csv(self) -> List[str]:
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


def run_extraction(
    csv_path: str,
    db_session,
    model_class,
    progress_callback: Optional[Callable] = None,
    headless: bool = True,
) -> Dict[str, int]:
    """
    Função helper para executar a extração de forma síncrona.

    Args:
        csv_path: Caminho para o arquivo CSV
        db_session: Sessão do SQLAlchemy
        model_class: Classe do modelo ExtractMogi
        progress_callback: Função opcional para reportar progresso
        headless: Se o browser deve rodar em modo headless

    Returns:
        Dict com estatísticas do processamento
    """
    processor = CSVProcessor(
        csv_path=csv_path,
        db_session=db_session,
        model_class=model_class,
        progress_callback=progress_callback,
        headless=headless,
    )

    # Executa o processamento assíncrono
    return asyncio.run(processor.process())
