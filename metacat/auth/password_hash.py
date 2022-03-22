import hashlib
from .py3 import to_bytes

PasswordHashAlgorithm = "sha1"

def password_hash(user, password):
    hashed = hashlib.new(PasswordHashAlgorithm)
    hashed.update(to_bytes(user))
    hashed.update(b":")
    hashed.update(to_bytes(password))
    return hashed.hexdigest()
