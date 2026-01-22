"""
SQLite storage layer for system and process memory metrics.
"""

import sqlite3
from typing import Any


def init_db(db_path: str) -> None:
    """
    Initialize the SQLite database with required tables and indexes.
    Idempotent: safe to call multiple times.
    
    Args:
        db_path: Path to the SQLite database file
        
    Raises:
        sqlite3.Error: On database initialization failure
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_snapshot (
                id INTEGER PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                mem_total_mb INTEGER NOT NULL,
                mem_used_mb INTEGER NOT NULL,
                mem_free_mb INTEGER NOT NULL,
                mem_compressed_mb INTEGER NOT NULL,
                swap_used_mb INTEGER NOT NULL,
                memory_pressure TEXT NOT NULL
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS process_snapshot (
                id INTEGER PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                pid INTEGER NOT NULL,
                process_name TEXT NOT NULL,
                rss_mb INTEGER NOT NULL,
                vms_mb INTEGER NOT NULL,
                shared_mb INTEGER NOT NULL,
                cpu_percent REAL NOT NULL,
                is_foreground INTEGER NOT NULL
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_system_timestamp 
            ON system_snapshot(timestamp)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_process_timestamp 
            ON process_snapshot(timestamp)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_process_pid 
            ON process_snapshot(pid)
        """)
        
        conn.commit()
    finally:
        conn.close()


def insert_system_snapshot(db_path: str, snapshot: dict[str, Any]) -> None:
    """
    Insert a single system memory snapshot.
    
    Args:
        db_path: Path to the SQLite database file
        snapshot: Dictionary with keys: timestamp, mem_total_mb, mem_used_mb,
                  mem_free_mb, mem_compressed_mb, swap_used_mb, memory_pressure
                  
    Raises:
        sqlite3.Error: On database write failure
        KeyError: If required snapshot keys are missing
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO system_snapshot (
                timestamp, mem_total_mb, mem_used_mb, mem_free_mb,
                mem_compressed_mb, swap_used_mb, memory_pressure
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot['timestamp'],
            snapshot['mem_total_mb'],
            snapshot['mem_used_mb'],
            snapshot['mem_free_mb'],
            snapshot['mem_compressed_mb'],
            snapshot['swap_used_mb'],
            snapshot['memory_pressure']
        ))
        conn.commit()
    finally:
        conn.close()


def insert_process_snapshots(db_path: str, snapshots: list[dict[str, Any]]) -> None:
    """
    Insert multiple process snapshots in a single transaction.
    
    Args:
        db_path: Path to the SQLite database file
        snapshots: List of dictionaries, each with keys: timestamp, pid,
                   process_name, rss_mb, vms_mb, shared_mb, cpu_percent,
                   is_foreground
                   
    Raises:
        sqlite3.Error: On database write failure
        KeyError: If required snapshot keys are missing
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO process_snapshot (
                timestamp, pid, process_name, rss_mb, vms_mb,
                shared_mb, cpu_percent, is_foreground
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                snap['timestamp'],
                snap['pid'],
                snap['process_name'],
                snap['rss_mb'],
                snap['vms_mb'],
                snap['shared_mb'],
                snap['cpu_percent'],
                snap['is_foreground']
            )
            for snap in snapshots
        ])
        conn.commit()
    finally:
        conn.close()
