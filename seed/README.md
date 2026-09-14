# Seed data

Edit `client_id` and `client_secret` in `seed.sql` with your FIB stage credentials before loading. Do not commit real values.

```bash
psql -h localhost -U payment -d payment_dev -f seed/seed.sql
# or: docker compose run --rm db-seed
```
