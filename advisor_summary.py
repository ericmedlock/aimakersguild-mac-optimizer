"""
Build advisor summary from database metrics.
"""

from typing import Any

from db_readers import (
    fetch_latest_snapshot,
    fetch_system_snapshots,
    fetch_top_processes,
)


def build_advisor_summary(db_path: str, window_minutes: int = 60) -> dict[str, Any]:
    """
    Build a summary of system memory metrics for advisor analysis.
    
    Args:
        db_path: Path to SQLite database
        window_minutes: Time window to analyze (default 60 minutes)
        
    Returns:
        Dictionary with memory statistics and process information
    """
    summary = {
        "window_minutes": window_minutes,
        "latest": None,
        "trends": {},
        "top_processes": [],
        "memory_pressure_history": [],
    }
    
    # Get latest snapshot
    try:
        latest = fetch_latest_snapshot(db_path)
        if latest:
            summary["latest"] = {
                "mem_used_mb": latest["mem_used_mb"],
                "mem_free_mb": latest["mem_free_mb"],
                "mem_compressed_mb": latest["mem_compressed_mb"],
                "swap_used_mb": latest["swap_used_mb"],
                "memory_pressure": latest["memory_pressure"],
                "timestamp": latest["timestamp"],
            }
    except RuntimeError:
        pass
    
    # Get historical snapshots for trends
    try:
        snapshots = fetch_system_snapshots(db_path, window_minutes)
        if snapshots:
            mem_used = [s["mem_used_mb"] for s in snapshots]
            mem_free = [s["mem_free_mb"] for s in snapshots]
            mem_compressed = [s["mem_compressed_mb"] for s in snapshots]
            swap_used = [s["swap_used_mb"] for s in snapshots]
            
            summary["trends"] = {
                "mem_used_avg": sum(mem_used) / len(mem_used) if mem_used else 0,
                "mem_used_max": max(mem_used) if mem_used else 0,
                "mem_used_min": min(mem_used) if mem_used else 0,
                "mem_free_avg": sum(mem_free) / len(mem_free) if mem_free else 0,
                "mem_compressed_avg": sum(mem_compressed) / len(mem_compressed) if mem_compressed else 0,
                "swap_used_avg": sum(swap_used) / len(swap_used) if swap_used else 0,
                "swap_used_max": max(swap_used) if swap_used else 0,
                "sample_count": len(snapshots),
            }
    except RuntimeError:
        pass
    
    # Get top processes
    try:
        processes = fetch_top_processes(db_path, window_minutes, limit=10)
        summary["top_processes"] = [
            {
                "process_name": p["process_name"],
                "pid": p["pid"],
                "max_rss_mb": round(p["max_rss_mb"], 1),
                "avg_rss_mb": round(p["avg_rss_mb"], 1),
                "times_seen": p["times_seen"],
                "foreground_ratio": round(p["foreground_ratio"], 2),
            }
            for p in processes
        ]
    except RuntimeError:
        pass
    
    return summary
