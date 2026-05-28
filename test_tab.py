from textual.app import App, ComposeResult
from textual.widgets import DataTable
from textual import events

class TableApp(App):
    BINDINGS = [
        ("c", "clear_table", "Clear Table"),
        ("a", "add_row", "Add Row")
    ]

    def compose(self) -> ComposeResult:
        yield DataTable(id="data_table")

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_columns("A", "B", "C")
        table.add_row("1", "2", "3")
        table.add_row("4", "5", "6")

    def action_clear_table(self) -> None:
        table = self.query_one("#data_table")
        table.clear()

    def action_add_row(self) -> None:
        table = self.query_one("#data_table")
        table.add_row("X", "Y", "Z")

if __name__ == "__main__":
    TableApp().run()
