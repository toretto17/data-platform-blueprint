# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly:

1. **DO NOT** open a public issue
2. Email: rshah4297.rs@gmail.com
3. Include: description, steps to reproduce, potential impact

## Response time

- Acknowledgment: within 48 hours
- Fix: within 7 days for critical issues

## Scope

This is a template repository. Security concerns may include:
- Hardcoded credentials in templates (should be `CHANGE_ME` placeholders only)
- Vulnerable dependencies in `requirements.txt`
- Insecure patterns in code templates

## Best practices enforced

- No hardcoded secrets (fail-fast on missing args)
- `detect-secrets` pre-commit hook blocks accidental credential commits
- API templates ship with authentication enabled by default
- All database access via Secrets Manager / secret scopes
