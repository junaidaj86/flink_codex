# Schema Resolution

- Use OAuth client credentials flow only
- Cache tokens by `(oauth_client_id, oauth_audience)`
- Never log secrets, tokens, or authorization headers
