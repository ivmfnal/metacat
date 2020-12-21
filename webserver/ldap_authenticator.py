import ldap

LDAP_SERVER_URL = 'ldaps://ldaps.fnal.gov'
LDAP_DN_TEMPLATE = 'cn=%s,ou=FermiUsers,dc=services,dc=fnal,dc=gov'

class LDAPAuthenticator(object):

    def __init__(self, server_url, dn_template='cn=%s,ou=FermiUsers,dc=services,dc=fnal,dc=gov'):
        self.URL = server_url
        self.DNTemplate = dn_template

    def authenticate(self, username, password):

        ld = ldap.initialize(self.URL)
        dn = self.DNTemplate % (username,)

        try:
            ld.simple_bind_s(dn, password)
        except ldap.INVALID_CREDENTIALS:
            return None
        return username

if __name__ == "__main__":
    import sys
    username, password = sys.argv[1:]
    a = LDAPAuthenticator(LDAP_SERVER_URL)
    print(a.authenticate(username, password))

