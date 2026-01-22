"""
System memory metrics collector for macOS.
"""

import os
import subprocess
import time
from typing import Any

import psutil
from dotenv import load_dotenv

from metrics_store import init_db, insert_process_snapshots, insert_system_snapshot

load_dotenv()

# Configuration
DB_PATH = os.getenv("DB_PATH", "system_metrics.db")
SAMPLE_INTERVAL_SEC = int(os.getenv("SAMPLE_INTERVAL_SEC", "5"))
TOP_N_PROCESSES = int(os.getenv("TOP_N_PROCESSES", "25"))


def run_command(cmd: list[str]) -> str:
    """Run shell command and return output."""
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def get_page_size() -> int:
    """Get system page size in bytes from vm_stat."""
    vm_stat_output = run_command(["vm_stat"])
    for line in vm_stat_output.split("\n"):
        if "page size of" in line:
            # Example: "Mach Virtual Memory Statistics: (page size of 16384 bytes)"
            parts = line.split()
            for i, part in enumerate(parts):
                if part == "of" and i + 1 < len(parts):
                    return int(parts[i + 1])
    return 4096  # fallback


def parse_vm_stat() -> dict[str, int]:
    """Parse vm_stat output to get page counts."""
    vm_stat_output = run_command(["vm_stat"])
    pages = {}
    
    for line in vm_stat_output.split("\n"):
        if ":" in line:
            parts = line.split(":")
            if len(parts) == 2:
                key = parts[0].strip()
                value_str = parts[1].strip().rstrip(".")
                try:
                    pages[key] = int(value_str)
                except ValueError:
                    continue
    
    return pages


def parse_swap_used_mb(swap_output: str) -> int:
    """
    Parse macOS `sysctl -n vm.swapusage` output and return used swap in MB.

    Expected format (example):
      "total = 1024.00M  used = 256.00M  free = 768.00M  (encrypted)"
    """
    tokens = swap_output.replace("=", " ").split()
    # Find the token after "used"
    for i, tok in enumerate(tokens):
        if tok.lower() == "used" and i + 1 < len(tokens):
            val = tokens[i + 1].strip()
            # val like "256.00M" or "1.50G"
            if val.endswith(("M", "m")):
                return int(float(val[:-1]))
            if val.endswith(("G", "g")):
                return int(float(val[:-1]) * 1024)
            # Fallback: try raw float MB
            return int(float(val))
    return 0


def get_system_metrics() -> dict[str, Any]:
    """Collect system memory metrics."""
    # Get total memory in bytes
    mem_total_bytes = int(run_command(["sysctl", "-n", "hw.memsize"]))
    mem_total_mb = mem_total_bytes // (1024 * 1024)
    
    # Get page size and vm_stat pages
    page_size = get_page_size()
    pages = parse_vm_stat()
    
    # Extract page counts
    free_pages = pages.get("Pages free", 0)
    active_pages = pages.get("Pages active", 0)
    inactive_pages = pages.get("Pages inactive", 0)
    wired_pages = pages.get("Pages wired down", 0)
    compressed_pages = pages.get("Pages occupied by compressor", 0)
    
    # Convert to MB
    free_mb = (free_pages * page_size) // (1024 * 1024)
    inactive_mb = (inactive_pages * page_size) // (1024 * 1024)
    active_mb = (active_pages * page_size) // (1024 * 1024)
    wired_mb = (wired_pages * page_size) // (1024 * 1024)
    compressed_mb = (compressed_pages * page_size) // (1024 * 1024)
    
    used_mb = active_mb + wired_mb
    
    # Get swap usage
    swap_output = run_command(["sysctl", "-n", "vm.swapusage"])
    swap_used_mb = parse_swap_used_mb(swap_output)
    
    # Compute memory pressure heuristic
    # High: free < 5% of total OR swap > 256MB
    # Medium: free < 10% of total OR swap > 0
    # Low: otherwise
    available_mb = free_mb + inactive_mb
    if free_mb < 0.05 * mem_total_mb or swap_used_mb > 256:
        pressure = "high"
    elif free_mb < 0.10 * mem_total_mb or swap_used_mb > 0:
        pressure = "medium"
    else:
        pressure = "low"
    
    return {
        "timestamp": int(time.time()),
        "mem_total_mb": mem_total_mb,
        "mem_used_mb": used_mb,
        "mem_free_mb": free_mb,
        "mem_compressed_mb": compressed_mb,
        "swap_used_mb": swap_used_mb,
        "memory_pressure": pressure,
    }


def get_frontmost_app_name() -> str | None:
    """Get the name of the frontmost application."""
    try:
        script = (
            'tell application "System Events" to get name of first application '
            'process whose frontmost is true'
        )
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=2,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None


def get_process_metrics(frontmost_app: str | None) -> list[dict[str, Any]]:
    """Collect top N process metrics by RSS."""
    timestamp = int(time.time())
    processes = []
    
    for proc in psutil.process_iter(['pid', 'name', 'memory_info', 'cpu_percent']):
        try:
            info = proc.info
            mem_info = info.get('memory_info')
            
            if mem_info is None:
                continue
            
            # Convert to MB
            rss_mb = mem_info.rss // (1024 * 1024)
            vms_mb = mem_info.vms // (1024 * 1024)
            
            # Get shared memory if available (may not be on all systems)
            shared_mb = 0
            if hasattr(mem_info, 'shared'):
                shared_mb = mem_info.shared // (1024 * 1024)
            
            # Check if foreground
            is_foreground = 0
            if frontmost_app and info['name'] == frontmost_app:
                is_foreground = 1
            
            processes.append({
                "timestamp": timestamp,
                "pid": info['pid'],
                "process_name": info['name'] or "unknown",
                "rss_mb": rss_mb,
                "vms_mb": vms_mb,
                "shared_mb": shared_mb,
                "cpu_percent": info['cpu_percent'] or 0.0,
                "is_foreground": is_foreground,
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    
    # Sort by RSS descending and take top N
    processes.sort(key=lambda p: p['rss_mb'], reverse=True)
    return processes[:TOP_N_PROCESSES]


def main() -> None:
    """Main collector loop."""
    init_db(DB_PATH)
    
    print(f"Starting metrics collector (interval={SAMPLE_INTERVAL_SEC}s, db={DB_PATH})")
    
    while True:
        try:
            # Collect system snapshot
            system_snapshot = get_system_metrics()
            insert_system_snapshot(DB_PATH, system_snapshot)
            
            # Collect process snapshots
            frontmost_app = get_frontmost_app_name()
            process_snapshots = get_process_metrics(frontmost_app)
            if process_snapshots:
                insert_process_snapshots(DB_PATH, process_snapshots)
            
            print(f"Collected snapshot at {system_snapshot['timestamp']}: "
                  f"{system_snapshot['mem_used_mb']}MB used, "
                  f"pressure={system_snapshot['memory_pressure']}, "
                  f"{len(process_snapshots)} processes")
            
        except Exception as e:
            print(f"Error in collection iteration: {e}")
        
        time.sleep(SAMPLE_INTERVAL_SEC)


if __name__ == "__main__":
    main()
