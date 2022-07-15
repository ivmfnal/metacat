import hashlib
from .py3 import to_bytes

PasswordHashAlgorithm = "sha1"

def password_hash(user, password):
    hashed = hashlib.new(PasswordHashAlgorithm)
    hashed.update(to_bytes(user))
    hashed.update(b":")
    hashed.update(to_bytes(password))
    return hashed.hexdigest()

def password_digest_hash(realm, user, password):
    # RFC2617 hash of the combination <user, realm, password>
    return hashlib.md5(to_bytes('%s:%s:%s' % (user, realm, password))).digest()
