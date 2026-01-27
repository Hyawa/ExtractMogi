"""
Widgets personalizados para a interface ExtractMogi.
"""

from textual.widgets import Header, Footer, DataTable, Static
from textual.containers import Vertical
from textual.app import ComposeResult


class ExtractMogiWidget(Static):
    """Widget principal da aplicação ExtractMogi."""

    def compose(self) -> ComposeResult:
        """Compõe os elementos do widget."""
        yield Header(show_clock=True)
        yield Vertical(
            Static("EXTRACTMOGI - EXTRATOR DE CONTATOS", id="title"),
            Static("Aguardando seleção de arquivo...", id="status_display"),
            Static("", id="progress_display"),
            DataTable(id="data_table"),
            id="main_container",
        )
        yield Footer()

    def on_mount(self) -> None:
        """Executado quando o widget é montado."""
        # Configura a tabela de dados
        table = self.query_one(DataTable)
        table.add_columns("Empresa", "Telefone", "Facebook", "Status")
        table.zebra_stripes = True
        table.cursor_type = "row"
        table.show_header = True
