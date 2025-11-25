import pymysql
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

    def __init__(self, db_config: dict = None):
        """
        Initialize repository with database configuration

        Args:
            db_config (dict): MySQL database configuration
        """
        if db_config is None:
            self.db_config = {
                'host': '192.168.5.178',
                'port': 33306,
                'user': 'quant',
                'password': 'quant',
                'database': 'quant',
                'charset': 'utf8mb4'
            }
        else:
            self.db_config = db_config

    def _get_connection(self):
        """
        Get a new MySQL database connection
        
        Returns:
            pymysql.Connection: MySQL database connection
        """
        return pymysql.connect(**self.db_config)

    def create(self, back_space: BackSpaceRecord) -> BackSpaceRecord:
        """
        Insert a new record into back_space table

        Args:
            back_space (BackSpaceRecord): BackSpace object to insert

        Returns:
            BackSpaceRecord: Created BackSpace object with assigned id
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO back_space (data, status) VALUES (%s, %s)",
                    (back_space.data, back_space.status)
                )
                conn.commit()
                back_space.id = cursor.lastrowid
        finally:
            conn.close()
        return back_space

    def get_by_id(self, id: int) -> Optional[BackSpaceRecord]:
        """
        Retrieve a record by its id

        Args:
            id (int): Record id

        Returns:
            Optional[BackSpaceRecord]: BackSpace object if found, None otherwise
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, data, status FROM back_space WHERE id = %s", (id,))
                row = cursor.fetchone()
                
            if row:
                return BackSpaceRecord(id=row[0], data=row[1], status=row[2])
            return None
        finally:
            conn.close()

    def get_all(self) -> List[BackSpaceRecord]:
        """
        Retrieve all records from back_space table

        Returns:
            List[BackSpaceRecord]: List of all BackSpace objects
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, data, status FROM back_space")
                rows = cursor.fetchall()
                
            return [BackSpaceRecord(id=row[0], data=row[1], status=row[2]) for row in rows]
        finally:
            conn.close()

    def update(self, back_space: BackSpaceRecord) -> bool:
        """
        Update a record in back_space table

        Args:
            back_space (BackSpaceRecord): BackSpace object with updated data

        Returns:
            bool: True if record was updated, False if record was not found
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE back_space SET data = %s, status = %s WHERE id = %s",
                    (back_space.data, back_space.status, back_space.id)
                )
                conn.commit()
                return cursor.rowcount > 0
        finally:
            conn.close()

    def delete(self, id: int) -> bool:
        """
        Delete a record from back_space table

        Args:
            id (int): Record id to delete

        Returns:
            bool: True if record was deleted, False if record was not found
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM back_space WHERE id = %s", (id,))
                conn.commit()
                return cursor.rowcount > 0
        finally:
            conn.close()