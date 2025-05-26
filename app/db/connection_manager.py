import json
import os
import getpass
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pathlib import Path
from typing import Optional

DB_CONFIG_PATH = Path(__file__).parent / "db_connections.json"

DB_PORT = 1433
DB_DRIVER = "ODBC Driver 17 for SQL Server"
DB_AUTH = "ActiveDirectoryInteractive"

class ConnectionManager:
    def __init__(self):
        self.connections = self.load_connections()
        self.active_connection: Optional[str] = None

    def load_connections(self):
        if DB_CONFIG_PATH.exists():
            with open(DB_CONFIG_PATH, "r") as f:
                return json.load(f)
        return {}

    def save_connections(self):
        with open(DB_CONFIG_PATH, "w") as f:
            json.dump(self.connections, f, indent=4)

    def list_connections(self):
        return list(self.connections.keys())

    def get_connection(self, name):
        return self.connections.get(name)

    def add_connection(self, name: str, server: str, db_name: str):
        if name in self.connections:
            raise ValueError("Connection already exists")
        self.connections[name] = {"server": server, "db_name": db_name}
        self.save_connections()

    def delete_connection(self, name: str):
        if name in self.connections:
            del self.connections[name]
            if self.active_connection == name:
                self.active_connection = None
            self.save_connections()
        else:
            raise ValueError("Connection not found")

    def select_connection(self, name: str):
        if name not in self.connections:
            raise ValueError("Connection not found")
        self.active_connection = name

    def get_selected_engine(self) -> Engine:
        if not self.active_connection:
            raise ValueError("No active connection selected")
        conn = self.connections[self.active_connection]
        return create_engine(self.build_connection_string(conn["server"], conn["db_name"]))

    def build_connection_string(self, server: str, db_name: str) -> str:
        user = f"{getpass.getuser()}@{os.environ['USERDOMAIN'].lower()}"
        encoded_driver = urllib.parse.quote_plus(DB_DRIVER)
        return (
            f"mssql+pyodbc://{user}@{server}:{DB_PORT}/{db_name}"
            f"?driver={encoded_driver}&authentication={DB_AUTH}"
        )
