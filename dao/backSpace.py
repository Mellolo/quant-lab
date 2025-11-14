import sqlite3
from typing import List, Optional

class BackSpaceRecord:
    """
    BackSpaceRecord model representing the back_space table structure
    """
    def __init__(self, id=None, data=None, status=None):
        self.id = id
        self.data = data
        self.status = status

    def to_dict(self):
        """
        Convert BackSpaceRecord object to dictionary
        """
        return {
            'id': self.id,
            'data': self.data,
            'status': self.status
        }

    @classmethod
    def from_dict(cls, data_dict):
        """
        Create BackSpaceRecord object from dictionary
        """
        return cls(
            id=data_dict.get('id'),
            data=data_dict.get('data'),
            status=data_dict.get('status')
        )


class BackSpaceRepository:
    """
    Repository class for accessing back_space table
    """

    def __init__(self, db_path: str = "backtest.db"):
        """
        Initialize repository with database path

        Args:
            db_path (str): Path to SQLite database file
        """
        self.db_path = db_path

    def create(self, back_space: BackSpaceRecord) -> BackSpaceRecord:
        """
        Insert a new record into back_space table

        Args:
            back_space (BackSpaceRecord): BackSpace object to insert

        Returns:
            BackSpaceRecord: Created BackSpace object with assigned id
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO back_space (data, status) VALUES (?, ?)",
                (back_space.data, back_space.status)
            )
            conn.commit()
            back_space.id = cursor.lastrowid
        return back_space

    def get_by_id(self, id: int) -> Optional[BackSpaceRecord]:
        """
        Retrieve a record by its id

        Args:
            id (int): Record id

        Returns:
            Optional[BackSpaceRecord]: BackSpace object if found, None otherwise
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, data, status FROM back_space WHERE id = ?", (id,))
            row = cursor.fetchone()

        if row:
            return BackSpaceRecord(id=row[0], data=row[1], status=row[2])
        return None

    def get_all(self) -> List[BackSpaceRecord]:
        """
        Retrieve all records from back_space table

        Returns:
            List[BackSpaceRecord]: List of all BackSpace objects
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, data, status FROM back_space")
            rows = cursor.fetchall()

        return [BackSpaceRecord(id=row[0], data=row[1], status=row[2]) for row in rows]

    def update(self, back_space: BackSpaceRecord) -> bool:
        """
        Update a record in back_space table

        Args:
            back_space (BackSpaceRecord): BackSpace object with updated data

        Returns:
            bool: True if record was updated, False if record was not found
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE back_space SET data = ?, status = ? WHERE id = ?",
                (back_space.data, back_space.status, back_space.id)
            )
            conn.commit()
            return cursor.rowcount > 0

    def delete(self, id: int) -> bool:
        """
        Delete a record from back_space table

        Args:
            id (int): Record id to delete

        Returns:
            bool: True if record was deleted, False if record was not found
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM back_space WHERE id = ?", (id,))
            conn.commit()
            return cursor.rowcount > 0