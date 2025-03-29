import xmlrpc.client
import pandas as pd
import logging
from typing import Dict, List, Union, Optional, Any, Tuple


class OdooConnector:
    """
    Menangani koneksi ke Odoo melalui XML-RPC.
    
    Attributes:
        host: URL server Odoo
        db: Nama database
        username: Username
        password: Password
        uid: User ID setelah login
        common_endpoint: Endpoint untuk common service
        object_endpoint: Endpoint untuk object service
    """
    
    def __init__(
        self,
        host: str,
        db: str,
        username: str,
        password: str,
        timeout: int = 120
    ):
        """
        Inisialisasi OdooConnector.
        
        Args:
            host: URL server Odoo (contoh: https://example.com)
            db: Nama database
            username: Username
            password: Password
            timeout: Timeout koneksi (detik)
        """
        # Normalize host (remove trailing slash if present)
        self.host = host.rstrip('/')
        self.db = db
        self.username = username
        self.password = password
        self.timeout = timeout
        
        # Initialize endpoints
        self.common_endpoint = None
        self.object_endpoint = None
        self.uid = None
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """
        Melakukan koneksi ke Odoo dan login.
        
        Returns:
            Boolean yang menunjukkan status koneksi
        """
        try:
            # Create a transport with timeout
            transport = xmlrpc.client.Transport()
            transport.timeout = self.timeout
            
            # Create endpoints
            self.common_endpoint = xmlrpc.client.ServerProxy(
                f'{self.host}/xmlrpc/2/common',
                transport=transport
            )
            self.object_endpoint = xmlrpc.client.ServerProxy(
                f'{self.host}/xmlrpc/2/object',
                transport=transport
            )
            
            # Check server info
            server_info = self.common_endpoint.version()
            self.logger.info(f"Connected to Odoo {server_info.get('server_version', 'Unknown')} at {self.host}")
            
            # Login to get uid
            self.uid = self.common_endpoint.authenticate(
                self.db, self.username, self.password, {}
            )
            
            if not self.uid:
                self.logger.error("Authentication failed. Please check credentials.")
                return False
            
            self.logger.info(f"Successfully authenticated as {self.username} (uid: {self.uid})")
            return True
            
        except Exception as e:
            self.logger.error(f"Connection error: {str(e)}")
            self.common_endpoint = None
            self.object_endpoint = None
            self.uid = None
            return False
    
    def is_connected(self) -> bool:
        """Check if connected and authenticated."""
        return self.uid is not None and self.object_endpoint is not None
    
    def search(
        self,
        model: str,
        domain: List,
        limit: Optional[int] = None,
        offset: int = 0,
        order: Optional[str] = None
    ) -> List[int]:
        """
        Search for records in Odoo.
        
        Args:
            model: Model name
            domain: Search domain
            limit: Maximum number of records to return
            offset: Number of records to skip
            order: Sort criteria
            
        Returns:
            List of record IDs
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to Odoo. Call connect() first.")
        
        try:
            kwargs = {}
            if limit is not None:
                kwargs['limit'] = limit
            if offset > 0:
                kwargs['offset'] = offset
            if order:
                kwargs['order'] = order
                
            record_ids = self.object_endpoint.execute_kw(
                self.db, self.uid, self.password,
                model, 'search',
                [domain], kwargs
            )
            
            return record_ids
        except Exception as e:
            self.logger.error(f"Search error for model {model}: {str(e)}")
            raise
    
    def read(
        self,
        model: str,
        record_ids: List[int],
        fields: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Read records from Odoo.
        
        Args:
            model: Model name
            record_ids: List of record IDs to read
            fields: List of fields to read (all if None)
            
        Returns:
            List of dictionaries with record data
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to Odoo. Call connect() first.")
        
        if not record_ids:
            return []
            
        try:
            kwargs = {}
            if fields:
                kwargs['fields'] = fields
                
            records = self.object_endpoint.execute_kw(
                self.db, self.uid, self.password,
                model, 'read',
                [record_ids], kwargs
            )
            
            return records
        except Exception as e:
            self.logger.error(f"Read error for model {model}: {str(e)}")
            raise
    
    def create(
        self,
        model: str,
        values: Dict
    ) -> int:
        """
        Create a record in Odoo.
        
        Args:
            model: Model name
            values: Dictionary of field values
            
        Returns:
            ID of created record
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to Odoo. Call connect() first.")
            
        try:
            record_id = self.object_endpoint.execute_kw(
                self.db, self.uid, self.password,
                model, 'create',
                [values]
            )
            
            return record_id
        except Exception as e:
            self.logger.error(f"Create error for model {model}: {str(e)}")
            raise
    
    def write(
        self,
        model: str,
        record_ids: List[int],
        values: Dict
    ) -> bool:
        """
        Update records in Odoo.
        
        Args:
            model: Model name
            record_ids: List of record IDs to update
            values: Dictionary of field values to update
            
        Returns:
            Boolean indicating success
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to Odoo. Call connect() first.")
            
        if not record_ids:
            return False
            
        try:
            result = self.object_endpoint.execute_kw(
                self.db, self.uid, self.password,
                model, 'write',
                [record_ids, values]
            )
            
            return result
        except Exception as e:
            self.logger.error(f"Write error for model {model}: {str(e)}")
            raise
    
    def unlink(
        self,
        model: str,
        record_ids: List[int]
    ) -> bool:
        """
        Delete records in Odoo.
        
        Args:
            model: Model name
            record_ids: List of record IDs to delete
            
        Returns:
            Boolean indicating success
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to Odoo. Call connect() first.")
            
        if not record_ids:
            return False
            
        try:
            result = self.object_endpoint.execute_kw(
                self.db, self.uid, self.password,
                model, 'unlink',
                [record_ids]
            )
            
            return result
        except Exception as e:
            self.logger.error(f"Unlink error for model {model}: {str(e)}")
            raise
    
    def bulk_create(
        self,
        model: str,
        df: pd.DataFrame,
        chunk_size: int = 100
    ) -> Tuple[List[int], List[Dict]]:
        """
        Create multiple records in Odoo from a DataFrame.
        
        Args:
            model: Model name
            df: DataFrame with data to create
            chunk_size: Number of records to create in each batch
            
        Returns:
            Tuple of (created_ids, errors)
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to Odoo. Call connect() first.")
            
        if df.empty:
            return [], []
            
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        created_ids = []
        errors = []
        
        # Process in chunks
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i+chunk_size]
            
            for record in chunk:
                try:
                    # Remove NaN/None values
                    clean_record = {k: v for k, v in record.items() if pd.notna(v)}
                    record_id = self.create(model, clean_record)
                    created_ids.append(record_id)
                except Exception as e:
                    errors.append({
                        'record': record,
                        'error': str(e)
                    })
                    self.logger.error(f"Error creating record: {str(e)}")
        
        return created_ids, errors
    
    def execute_method(
        self,
        model: str,
        method: str,
        args: List = None,
        kwargs: Dict = None
    ) -> Any:
        """
        Execute a custom method on an Odoo model.
        
        Args:
            model: Model name
            method: Method name
            args: Positional arguments
            kwargs: Keyword arguments
            
        Returns:
            Result of method execution
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to Odoo. Call connect() first.")
            
        try:
            args = args or []
            kwargs = kwargs or {}
            
            result = self.object_endpoint.execute_kw(
                self.db, self.uid, self.password,
                model, method, args, kwargs
            )
            
            return result
        except Exception as e:
            self.logger.error(f"Method execution error for {model}.{method}: {str(e)}")
            raise
