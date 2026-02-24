import hashlib
import os


def file_hash(file_path: str) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

def save_hash(file_path: str, hash_path: str) -> None:
    file_h = file_hash(file_path)
    with open(hash_path, "w") as f:
        f.write(file_h)

def verify_file_hash(file_path: str, hash_path: str) -> bool:
    if not os.path.exists(hash_path) or not os.path.exists(file_path):
        return False
    actual_hash = file_hash(file_path)
    with open(hash_path, "r") as f:
        expected_hash = f.read().strip()
    return actual_hash == expected_hash

