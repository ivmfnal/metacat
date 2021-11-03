import ldap

LDAP_SERVER_URL = 'ldaps://ldaps.fnal.gov'
LDAP_DN_TEMPLATE = 'cn=%s,ou=FermiUsers,dc=services,dc=fnal,dc=gov'

class LDAPAuthenticator(object):

    def __init__(self, server_url):
        self.URL = server_url

    def authenticate(self, dn, password):

        ld = ldap.initialize(self.URL)
        try:
            ld.simple_bind_s(dn, password)
        except ldap.INVALID_CREDENTIALS:
            return False
        return True

if __name__ == "__main__":
    import sys
    username, password = sys.argv[1:]
    a = LDAPAuthenticator(LDAP_SERVER_URL)
    print(a.authenticate(LDAP_DN_TEMPLATE % (username,), password))

