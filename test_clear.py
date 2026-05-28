from textual.app import App, ComposeResult
from textual.widgets import DataTable

class TestApp(App):
    def compose(self) -> ComposeResult:
        yield DataTable(id="data_table")

    def on_mount(self) -> None:
        table = self.query_one("#data_table", DataTable)
        table.add_columns("A", "B", "C")
        table.add_row("1", "2", "3")
        table.add_row("4", "5", "6")
        
        print(f"Before clear: {len(table.rows)} rows")
        table.clear()
        print(f"After clear: {len(table.rows)} rows")
        self.exit()

if __name__ == "__main__":
    app = TestApp()
    app.run(headless=True)
