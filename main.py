"""
ExtractMogi - Aplicação Principal
Sistema de extração automatizada de contatos de empresas.
"""

import os
import tkinter as tk
from tkinter import filedialog
from pathlib import Path
import asyncio
import logging

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.worker import Worker, WorkerState
from textual import work

from src.ui.widgets import ExtractMogiWidget
from src.database.db_handler import SessionLocal, engine, Base
from src.database.models import ExtractMogi
from src.processors.async_processor import AsyncCSVProcessor
from src.exporters.data_exporter import DataExporter

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("extractmogi.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class ExtractMogiApp(App):
    """Aplicação principal do ExtractMogi com interface Textual."""

    CSS = """
    #main_container { 
        padding: 1; 
    }
    
    #title {
        text-align: center;
        text-style: bold;
        background: $accent;
        color: white;
        margin-bottom: 1;
        padding: 1;
    }
    
    #status_display {
        background: $surface;
        color: $text;
        padding: 1;
        margin-bottom: 1;
        border: tall $primary;
        text-align: center;
    }
    
    #progress_display {
        background: $surface;
        color: $text;
        padding: 1;
        margin-bottom: 1;
        border: tall $success;
        text-align: center;
    }
    
    DataTable {
        height: 100%;
    }
    """

    BINDINGS = [
        Binding("i", "import_file", "Importar CSV", show=True),
        Binding("p", "process", "Processar", show=True),
        Binding("e", "export", "Exportar", show=True),
        Binding("c", "clear_table", "Limpar Tabela", show=True),
        Binding("q", "quit", "Sair", show=True),
    ]

    def __init__(self):
        super().__init__()
        self.selected_file = None
        self.db_session = None
        self.processing = False
        self.stats = {}

        # Cria as tabelas no banco se não existirem
        Base.metadata.create_all(bind=engine)
        logger.info("Banco de dados inicializado")

    def compose(self) -> ComposeResult:
        """Compõe a interface da aplicação."""
        yield ExtractMogiWidget()

    def on_mount(self) -> None:
        """Executado quando a aplicação é montada."""
        self.update_status("Aguardando seleção de arquivo... [Pressione I]")

    def action_import_file(self) -> None:
        """
        Ação para importar arquivo CSV.
        Abre o diálogo de seleção de arquivo do Windows.
        """
        if self.processing:
            self.update_status("❌ Aguarde o processamento atual finalizar")
            return

        try:
            # Inicializa o tkinter sem mostrar a janela principal
            root = tk.Tk()
            root.withdraw()
            root.wm_attributes("-topmost", True)

            # Abre a janela de seleção de arquivo do Windows
            file_path = filedialog.askopenfilename(
                title="Selecione o arquivo CSV de empresas",
                filetypes=[("Arquivos CSV", "*.csv"), ("Todos os arquivos", "*.*")],
                initialdir=os.path.join(os.getcwd(), "data"),
            )

            root.destroy()

            if file_path:
                self._load_csv_file(file_path)
            else:
                self.update_status("⚠ Seleção cancelada")

        except Exception as e:
            logger.error(f"Erro ao importar arquivo: {e}")
            self.update_status(f"❌ Erro ao importar: {str(e)}")

    def _load_csv_file(self, file_path: str) -> None:
        """
        Carrega e valida o arquivo CSV selecionado.

        Args:
            file_path: Caminho do arquivo CSV
        """
        try:
            filename = os.path.basename(file_path)
            self.selected_file = file_path

            # Cria uma sessão temporária para validar o CSV
            temp_session = SessionLocal()
            processor = AsyncCSVProcessor(
                csv_path=file_path,
                db_session=temp_session,
                model_class=ExtractMogi,
                headless=True,
            )

            # Lê e valida o CSV
            companies = processor.read_csv()
            temp_session.close()

            # Atualiza a UI
            self.update_status(
                f"✓ Arquivo carregado: {filename} ({len(companies)} empresas)"
            )
            self.update_progress(f"Pronto para processar. Pressione [P] para iniciar.")

            logger.info(f"CSV carregado: {filename} com {len(companies)} empresas")

        except FileNotFoundError as e:
            self.update_status(f"❌ Arquivo não encontrado: {str(e)}")
            self.selected_file = None
        except Exception as e:
            logger.error(f"Erro ao carregar CSV: {e}")
            self.update_status(f"❌ Erro ao carregar CSV: {str(e)}")
            self.selected_file = None

    def action_process(self) -> None:
        """
        Ação para processar o arquivo CSV.
        Inicia o processamento assíncrono em background.
        """
        if not self.selected_file:
            self.update_status("❌ Selecione um arquivo primeiro [Pressione I]")
            return

        if self.processing:
            self.update_status("⚠ Processamento já em andamento...")
            return

        # Marca como processando
        self.processing = True
        self.update_status(f"🚀 Iniciando processamento...")

        # Inicia o processamento assíncrono
        self.process_companies()

    @work(exclusive=True, thread=True)
    async def process_companies(self) -> None:
        """
        Worker assíncrono para processar empresas em background.
        Utiliza o decorador @work do Textual para não travar a UI.
        """
        try:
            logger.info("Iniciando processamento de empresas")

            # Cria nova sessão do banco
            self.db_session = SessionLocal()

            # Cria o processador
            processor = AsyncCSVProcessor(
                csv_path=self.selected_file,
                db_session=self.db_session,
                model_class=ExtractMogi,
                headless=True,  # Mude para False para debug visual
            )

            # Define callbacks para atualizar a UI
            processor.set_callbacks(
                on_progress=self._on_progress,
                on_company_start=self._on_company_start,
                on_company_complete=self._on_company_complete,
                on_error=self._on_error,
                on_captcha_detected=self._on_captcha_detected,
            )

            # Executa o processamento
            self.stats = await processor.process_all()

            # Processamento concluído
            await self._on_processing_complete()

        except Exception as e:
            logger.error(f"Erro durante processamento: {e}")
            self.call_from_thread(
                self.update_status, f"❌ Erro durante processamento: {str(e)}"
            )
        finally:
            # Fecha a sessão do banco
            if self.db_session:
                self.db_session.close()
            self.processing = False

    async def _on_progress(self, current: int, total: int, percentage: int) -> None:
        """
        Callback de progresso.

        Args:
            current: Número atual
            total: Total de empresas
            percentage: Percentual completo
        """
        progress_text = f"Progresso: {current}/{total} ({percentage}%)"
        self.call_from_thread(self.update_progress, progress_text)

    async def _on_company_start(self, nome_empresa: str) -> None:
        """
        Callback quando inicia processamento de uma empresa.

        Args:
            nome_empresa: Nome da empresa
        """
        status_text = f"🔍 Processando: {nome_empresa}"
        self.call_from_thread(self.update_status, status_text)
        logger.info(f"Iniciando: {nome_empresa}")

    async def _on_company_complete(self, nome_empresa: str, data: dict) -> None:
        """
        Callback quando completa processamento de uma empresa.

        Args:
            nome_empresa: Nome da empresa
            data: Dados extraídos
        """
        # Determina o status baseado nos dados encontrados
        status_parts = []

        if data.get("telefone"):
            status_parts.append("Tel✓")
        if data.get("site"):
            status_parts.append("Site✓")
        if data.get("facebook_link"):
            status_parts.append("FB✓")
        if data.get("email"):
            status_parts.append("Email✓")
        if data.get("celular_whatsapp"):
            status_parts.append("WhatsApp✓")

        status = " | ".join(status_parts) if status_parts else "Sem dados"

        # Adiciona linha na tabela
        self.call_from_thread(
            self._add_table_row,
            nome_empresa,
            data.get("telefone", "—"),
            data.get("facebook_link", "—"),
            status,
        )

        logger.info(f"Concluído: {nome_empresa} - {status}")

    async def _on_error(self, nome_empresa: str, error_message: str) -> None:
        """
        Callback quando ocorre erro no processamento.

        Args:
            nome_empresa: Nome da empresa
            error_message: Mensagem de erro
        """
        # Adiciona linha na tabela com erro
        self.call_from_thread(self._add_table_row, nome_empresa, "—", "—", f"❌ Erro")

        logger.error(f"Erro em {nome_empresa}: {error_message}")

    async def _on_captcha_detected(self, nome_empresa: str, message: str) -> None:
        """
        Callback quando CAPTCHA é detectado.

        Args:
            nome_empresa: Nome da empresa
            message: Mensagem sobre o CAPTCHA
        """
        status_text = f"🤖 CAPTCHA DETECTADO para: {nome_empresa}"
        self.call_from_thread(self.update_status, status_text)

        # Adiciona linha na tabela indicando CAPTCHA
        self.call_from_thread(self._add_table_row, nome_empresa, "—", "—", "🤖 CAPTCHA")

        logger.warning(f"CAPTCHA: {nome_empresa} - {message}")

    async def _on_processing_complete(self) -> None:
        """Callback quando todo o processamento é concluído."""
        stats = self.stats

        summary = (
            f"✓ Processamento concluído! | "
            f"Total: {stats['total']} | "
            f"Sucesso: {stats['com_dados']} | "
            f"Sem dados: {stats['sem_dados']} | "
            f"Erros: {stats['erros']}"
        )

        self.call_from_thread(self.update_status, summary)
        self.call_from_thread(
            self.update_progress, "Pressione [E] para exportar ou [I] para novo arquivo"
        )

        logger.info(f"Processamento finalizado: {stats}")

    def _add_table_row(
        self, empresa: str, telefone: str, facebook: str, status: str
    ) -> None:
        """
        Adiciona uma linha na tabela de dados.

        Args:
            empresa: Nome da empresa
            telefone: Telefone
            facebook: Link do Facebook
            status: Status do processamento
        """
        try:
            table = self.query_one("#data_table")

            # Trunca valores muito longos
            telefone_short = telefone[:20] if telefone else "—"
            facebook_short = "Link FB" if facebook and facebook != "—" else "—"

            table.add_row(empresa, telefone_short, facebook_short, status)
        except Exception as e:
            logger.error(f"Erro ao adicionar linha na tabela: {e}")

    def action_export(self) -> None:
        """
        Ação para exportar dados.
        Gera CSV com filtro de URI (apenas empresas com site ou Facebook).
        """
        if self.processing:
            self.update_status("⚠ Aguarde o processamento finalizar")
            return

        try:
            self.update_status("📊 Gerando exportação...")

            # Cria sessão do banco
            db_session = SessionLocal()

            # Cria o exportador
            exporter = DataExporter(
                db_session=db_session, model_class=ExtractMogi, export_dir="exports"
            )

            # Exporta com filtro de URI
            filepath = exporter.export_with_uri_filter()

            # Fecha a sessão
            db_session.close()

            if filepath:
                filename = os.path.basename(filepath)
                self.update_status(f"✓ Exportação concluída: {filename}")
                self.update_progress(f"Arquivo salvo em: {filepath}")
                logger.info(f"Exportação realizada: {filepath}")
            else:
                self.update_status("⚠ Nenhuma empresa com URI para exportar")

        except Exception as e:
            logger.error(f"Erro na exportação: {e}")
            self.update_status(f"❌ Erro na exportação: {str(e)}")

    def action_clear_table(self) -> None:
        """Limpa a tabela de dados."""
        try:
            table = self.query_one("#data_table")
            table.clear()
            self.update_status("🗑 Tabela limpa")
            logger.info("Tabela de dados limpa")
        except Exception as e:
            logger.error(f"Erro ao limpar tabela: {e}")

    def update_status(self, message: str) -> None:
        """
        Atualiza o display de status.

        Args:
            message: Mensagem para exibir
        """
        try:
            status_display = self.query_one("#status_display")
            status_display.update(message)
        except Exception as e:
            logger.error(f"Erro ao atualizar status: {e}")

    def update_progress(self, message: str) -> None:
        """
        Atualiza o display de progresso.

        Args:
            message: Mensagem de progresso
        """
        try:
            progress_display = self.query_one("#progress_display")
            progress_display.update(message)
        except Exception as e:
            logger.error(f"Erro ao atualizar progresso: {e}")


def main():
    """Função principal para iniciar a aplicação."""
    logger.info("Iniciando ExtractMogi")
    app = ExtractMogiApp()
    app.run()


if __name__ == "__main__":
    main()
