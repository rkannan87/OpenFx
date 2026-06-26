
VaultPanDecryptService is an outbound HTTP client, not a web application serving responses. HSTS is a response header set by servers; a client cannot set it on a request write. Transport is TLS-enforced via openSecureConnection, and the code already inspects the server's Strict-Transport-Security header and warns if absent. Missing-HSTS-Header query not applicable to client-side code.
