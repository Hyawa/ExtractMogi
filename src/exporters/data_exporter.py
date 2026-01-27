"""
Módulo responsável pela exportação de dados para CSV.
"""

import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


class DataExporter:
    """Exporta dados do banco para CSV com filtros aplicados."""

    def __init__(self, db_session, model_class, export_dir: str = "exports"):
        """
        Inicializa o exportador.

        Args:
            db_session: Sessão do SQLAlchemy
            model_class: Classe do modelo ExtractMogi
            export_dir: Diretório onde os CSVs serão salvos
        """
        self.db_session = db_session
        self.model_class = model_class
        self.export_dir = Path(export_dir)

        # Cria o diretório de exportação se não existir
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def export_with_uri_filter(self) -> str:
        """
        Exporta apenas empresas que possuem pelo menos uma URI válida
        (site ou facebook_link).

        REGRA DE NEGÓCIO: Apenas estabelecimentos com link de Site OU
        link de Facebook serão incluídos no CSV exportado.

        Returns:
            Caminho do arquivo CSV gerado
        """
        try:
            # Query com filtro: site OU facebook_link devem estar preenchidos
            companies = (
                self.db_session.query(self.model_class)
                .filter(
                    (self.model_class.site.isnot(None))
                    | (self.model_class.facebook_link.isnot(None))
                )
                .all()
            )

            if not companies:
                logger.warning("Nenhuma empresa com URI encontrada para exportação")
                return None

            # Gera nome do arquivo com timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"extractmogi_export_{timestamp}.csv"
            filepath = self.export_dir / filename

            # Prepara dados para exportação
            data_to_export = []
            for company in companies:
                data_to_export.append(
                    {
                        "Nome Empresa": company.nome_empresa,
                        "Telefone": company.telefone or "",
                        "Celular/WhatsApp": company.celular_whatsapp or "",
                        "Facebook": company.facebook_link or "",
                        "Email": company.email or "",
                        "Site": company.site or "",
                        "Data Extração": company.data_extracao.strftime(
                            "%d/%m/%Y %H:%M:%S"
                        ),
                    }
                )

            # Escreve o CSV
            self._write_csv(filepath, data_to_export)

            logger.info(
                f"Exportação concluída: {len(companies)} empresas exportadas para {filepath}"
            )
            return str(filepath)

        except Exception as e:
            logger.error(f"Erro na exportação: {str(e)}")
            raise

    def export_all(self) -> str:
        """
        Exporta todas as empresas do banco (sem filtros).

        Returns:
            Caminho do arquivo CSV gerado
        """
        try:
            companies = self.db_session.query(self.model_class).all()

            if not companies:
                logger.warning("Nenhuma empresa encontrada no banco para exportação")
                return None

            # Gera nome do arquivo com timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"extractmogi_full_export_{timestamp}.csv"
            filepath = self.export_dir / filename

            # Prepara dados para exportação
            data_to_export = []
            for company in companies:
                data_to_export.append(
                    {
                        "Nome Empresa": company.nome_empresa,
                        "Telefone": company.telefone or "",
                        "Celular/WhatsApp": company.celular_whatsapp or "",
                        "Facebook": company.facebook_link or "",
                        "Email": company.email or "",
                        "Site": company.site or "",
                        "Data Extração": company.data_extracao.strftime(
                            "%d/%m/%Y %H:%M:%S"
                        ),
                    }
                )

            # Escreve o CSV
            self._write_csv(filepath, data_to_export)

            logger.info(
                f"Exportação completa: {len(companies)} empresas exportadas para {filepath}"
            )
            return str(filepath)

        except Exception as e:
            logger.error(f"Erro na exportação: {str(e)}")
            raise

    def get_export_statistics(self) -> Dict[str, int]:
        """
        Retorna estatísticas sobre os dados disponíveis para exportação.

        Returns:
            Dict com estatísticas
        """
        try:
            total = self.db_session.query(self.model_class).count()

            with_uri = (
                self.db_session.query(self.model_class)
                .filter(
                    (self.model_class.site.isnot(None))
                    | (self.model_class.facebook_link.isnot(None))
                )
                .count()
            )

            with_email = (
                self.db_session.query(self.model_class)
                .filter(self.model_class.email.isnot(None))
                .count()
            )

            with_whatsapp = (
                self.db_session.query(self.model_class)
                .filter(self.model_class.celular_whatsapp.isnot(None))
                .count()
            )

            with_phone = (
                self.db_session.query(self.model_class)
                .filter(self.model_class.telefone.isnot(None))
                .count()
            )

            return {
                "total": total,
                "com_uri": with_uri,
                "com_email": with_email,
                "com_whatsapp": with_whatsapp,
                "com_telefone": with_phone,
                "sem_uri": total - with_uri,
            }

        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {str(e)}")
            return {}

    @staticmethod
    def _write_csv(filepath: Path, data: List[Dict]):
        """
        Escreve dados em arquivo CSV.

        Args:
            filepath: Caminho do arquivo
            data: Lista de dicionários com os dados
        """
        if not data:
            return

        with open(filepath, "w", newline="", encoding="utf-8-sig") as csvfile:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(data)


def export_data_with_uri_filter(
    db_session, model_class, export_dir: str = "exports"
) -> str:
    """
    Função helper para exportar dados com filtro de URI.

    Args:
        db_session: Sessão do SQLAlchemy
        model_class: Classe do modelo ExtractMogi
        export_dir: Diretório de exportação

    Returns:
        Caminho do arquivo CSV gerado
    """
    exporter = DataExporter(db_session, model_class, export_dir)
    return exporter.export_with_uri_filter()


def get_export_stats(db_session, model_class) -> Dict[str, int]:
    """
    Função helper para obter estatísticas de exportação.

    Args:
        db_session: Sessão do SQLAlchemy
        model_class: Classe do modelo ExtractMogi

    Returns:
        Dict com estatísticas
    """
    exporter = DataExporter(db_session, model_class)
    return exporter.get_export_statistics()
