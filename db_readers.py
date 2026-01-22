"""
Database reader functions for dashboard.
"""

import sqlite3
from typing import Any


def validate_schema(db_path: str) -> None:
    """
    Validate that the database has the required tables and columns.
    
    Args:
        db_path: Path to the SQLite database file
        
    Raises:
        RuntimeError: If schema validation fails
    """
    required_tables = {
        'system_snapshot': {
            'id', 'timestamp', 'mem_total_mb', 'mem_used_mb', 
            'mem_free_mb', 'mem_compressed_mb', 'swap_used_mb', 'memory_pressure'
        },
        'process_snapshot': {
            'id', 'timestamp', 'pid', 'process_name', 
            'rss_mb', 'vms_mb', 'shared_mb', 'cpu_percent', 'is_foreground'
        }
    }
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        for table_name, expected_columns in required_tables.items():
            cursor.execute(f"PRAGMA table_info({table_name})")
            rows = cursor.fetchall()
            
            if not rows:
                raise RuntimeError(f"Table '{table_name}' does not exist")
            
            actual_columns = {row[1] for row in rows}
            missing_columns = expected_columns - actual_columns
            
            if missing_columns:
                raise RuntimeError(
                    f"Table '{table_name}' missing columns: {missing_columns}"
                )
        
        conn.close()
    except sqlite3.Error as e:
        raise RuntimeError(f"Database error during schema validation: {e}")


def fetch_latest_snapshot(db_path: str) -> dict[str, Any] | None:
    """
    Fetch the most recent system snapshot.
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        Dictionary with snapshot data or None if no data exists
        
    Raises:
        RuntimeError: On database read failure
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT timestamp, mem_total_mb, mem_used_mb, mem_free_mb,
                   mem_compressed_mb, swap_used_mb, memory_pressure
            FROM system_snapshot
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return dict(row)
        return None
    except sqlite3.Error as e:
        raise RuntimeError(f"Database error fetching latest snapshot: {e}")


def fetch_system_snapshots(db_path: str, window_minutes: int) -> list[dict[str, Any]]:
    """
    Fetch system snapshots within the specified time window.
    
    Args:
        db_path: Path to the SQLite database file
        window_minutes: Number of minutes to look back from now
        
    Returns:
        List of snapshot dictionaries, ordered by timestamp ascending
        
    Raises:
        RuntimeError: On database read failure
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT timestamp, mem_used_mb, mem_free_mb, mem_compressed_mb, swap_used_mb
            FROM system_snapshot
            WHERE timestamp >= unixepoch('now') - (? * 60)
            ORDER BY timestamp ASC
        """, (window_minutes,))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    except sqlite3.Error as e:
        raise RuntimeError(f"Database error fetching system snapshots: {e}")


def fetch_top_processes(db_path: str, window_minutes: int, limit: int = 25) -> list[dict[str, Any]]:
    """
    Fetch top processes by memory usage within the specified time window.
    
    Args:
        db_path: Path to the SQLite database file
        window_minutes: Number of minutes to look back from now
        limit: Maximum number of processes to return
        
    Returns:
        List of process dictionaries with aggregated metrics, ordered by max_rss_mb descending
        
    Raises:
        RuntimeError: On database read failure
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                process_name,
                pid,
                MAX(rss_mb) as max_rss_mb,
                AVG(rss_mb) as avg_rss_mb,
                COUNT(*) as times_seen,
                AVG(CAST(is_foreground AS REAL)) as foreground_ratio
            FROM process_snapshot
            WHERE timestamp >= unixepoch('now') - (? * 60)
            GROUP BY process_name, pid
            ORDER BY max_rss_mb DESC
            LIMIT ?
        """, (window_minutes, limit))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    except sqlite3.Error as e:
        raise RuntimeError(f"Database error fetching top processes: {e}")
