"""
Shared utilities for 18-749 Milestone 1.
Provides timestamped, color-coded console output.
"""

import datetime
from colorama import Fore, Style, init

init(autoreset=True)

COLORS = {
    "heartbeat":   Fore.CYAN,
    "request":     Fore.GREEN,
    "reply":       Fore.YELLOW,
    "state":       Fore.MAGENTA,
    "fault":       Fore.RED + Style.BRIGHT,
    "membership":  Fore.BLUE + Style.BRIGHT,
    "info":        Fore.WHITE,
    "duplicate":   Fore.RED,
}


def ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def log(msg: str, category: str = "info") -> None:
    color = COLORS.get(category, Fore.WHITE)
    print(f"{color}[{ts()}] {msg}{Style.RESET_ALL}", flush=True)