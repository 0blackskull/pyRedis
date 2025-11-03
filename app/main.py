import socket  # noqa: F401
import selectors
import types
from typing import List, Union
import time
import random

from .server import event_loop

if __name__ == "__main__":
    event_loop()