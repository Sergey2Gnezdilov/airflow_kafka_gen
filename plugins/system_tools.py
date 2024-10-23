from os import access, W_OK, R_OK
from pathlib import Path


def list_parents(folder_path, hops=2):
    folder_path = Path(folder_path)
    result = []
    while hops >= 0:
        result.append(folder_path)
        folder_path = folder_path.parent.absolute()
        hops -= 1
    return result


def check_read_write_permission(folder_path):
    return access(folder_path, R_OK), access(folder_path, W_OK)
