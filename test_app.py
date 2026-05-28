import asyncio
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, DataTable, Static
from textual.containers import Vertical
from textual import work

class ExtractMogiWidget(Static):
    def compose(self) -> ComposeResult:
        yield Vertical(
            DataTable(id="data_table"),
            id="main_container",
        )

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_columns("A", "B", "C")
        table.add_row("1", "2", "3")
        table.add_row("4", "5", "6")

class TestRealApp(App):
    BINDINGS = [
        ("c", "clear_table", "Limpar Tabela"),
    ]

    def compose(self) -> ComposeResult:
        yield ExtractMogiWidget()

    def action_clear_table(self) -> None:
        table = self.query_one("#data_table")
        table.clear()
        print(f"Cleared! Rows left: {len(table.rows)}")
        self.exit()

if __name__ == "__main__":
    app = TestRealApp()
    app.run(headless=True)
