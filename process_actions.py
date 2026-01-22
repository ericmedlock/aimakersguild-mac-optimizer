"""
Safe process termination helpers.
"""

import os
from typing import Any

import psutil


PROTECTED_NAMES = {"launchd", "kernel_task", "WindowServer", "loginwindow"}


def can_kill_pid(pid: int) -> tuple[bool, str]:
    """
    Check if a process can be safely terminated.
    
    Args:
        pid: Process ID to check
        
    Returns:
        Tuple of (allowed, reason)
        allowed=True with reason="ok" if termination is allowed
        allowed=False with reason describing why termination is denied
    """
    if pid <= 1:
        return (False, "system process pid <= 1")
    
    if pid == os.getpid():
        return (False, "cannot kill self")
    
    try:
        proc = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return (False, "process does not exist")
    except psutil.AccessDenied:
        return (False, "access denied")
    
    try:
        name = proc.name()
        if name in PROTECTED_NAMES:
            return (False, f"protected process: {name}")
    except psutil.AccessDenied:
        return (False, "access denied")
    
    try:
        uids = proc.uids()
        if uids.real == 0 and os.geteuid() != 0:
            return (False, "root-owned process, non-root user")
    except psutil.AccessDenied:
        return (False, "access denied")
    
    return (True, "ok")


def terminate_pid(pid: int, force: bool = False, timeout_sec: float = 3.0) -> dict[str, Any]:
    """
    Terminate a process after safety checks.
    
    Args:
        pid: Process ID to terminate
        force: If False, use SIGTERM; if True, use SIGKILL
        timeout_sec: Seconds to wait for process to exit
        
    Returns:
        Dictionary with keys:
        - ok: bool, True if terminated successfully
        - pid: int, the process ID
        - action: str, one of: "deny", "terminate", "kill", "no_such_process"
        - reason: str, explanation of outcome
    """
    allowed, reason = can_kill_pid(pid)
    
    if not allowed:
        return {
            "ok": False,
            "pid": pid,
            "action": "deny",
            "reason": reason
        }
    
    try:
        proc = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return {
            "ok": False,
            "pid": pid,
            "action": "no_such_process",
            "reason": "process does not exist"
        }
    
    action = "kill" if force else "terminate"
    
    try:
        if force:
            proc.kill()
        else:
            proc.terminate()
        
        try:
            proc.wait(timeout=timeout_sec)
            return {
                "ok": True,
                "pid": pid,
                "action": action,
                "reason": "terminated successfully"
            }
        except psutil.TimeoutExpired:
            return {
                "ok": False,
                "pid": pid,
                "action": action,
                "reason": f"process did not exit within {timeout_sec}s"
            }
    except psutil.NoSuchProcess:
        return {
            "ok": True,
            "pid": pid,
            "action": action,
            "reason": "process already gone"
        }
    except psutil.AccessDenied:
        return {
            "ok": False,
            "pid": pid,
            "action": action,
            "reason": "access denied"
        }
    except Exception as e:
        return {
            "ok": False,
            "pid": pid,
            "action": action,
            "reason": f"error: {type(e).__name__}"
        }
