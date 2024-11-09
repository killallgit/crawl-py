import sqlite3
import json
import mmap

class JSONLDatabase:
    def __init__(self, jsonl_path):
        self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE entries (
                key TEXT PRIMARY KEY, 
                value TEXT
            )
        ''')
        self._load_jsonl(jsonl_path)

    def _load_jsonl(self, path):
        with open(path, 'r') as f:
            for line in f:
                entry = json.loads(line)
                key = entry.get('id')  # Adjust key as needed
                self.cursor.execute(
                    'INSERT OR REPLACE INTO entries VALUES (?, ?)', 
                    (key, json.dumps(entry))
                )
        self.conn.commit()

    def get(self, key):
        self.cursor.execute('SELECT value FROM entries WHERE key = ?', (key,))
        result = self.cursor.fetchone()
        return json.loads(result[0]) if result else None

    def update(self, key, updated_data):
        self.cursor.execute(
            'UPDATE entries SET value = ? WHERE key = ?', 
            (json.dumps(updated_data), key)
        )
        self.conn.commit()

