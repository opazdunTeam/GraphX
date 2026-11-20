# Развёртывание
- локальные infrastructure integration tests используют Docker Compose из каталогов компонентов;
- reverse proxy нужен только для необязательной внешней демонстрации;
- существующий `graphx-landing.service` относится к статическому лендингу и не задаёт архитектуру GraphX API/Web;
- production Compose и blue-green добавляются только при наличии времени после основного сценария.

См. [качество, безопасность и эксплуатацию](../docs/14-quality-security-operations.md).

Тест immutable MinIO storage запускается вместе с RabbitMQ harness:

```bash
cd workers
uv run --locked python tools/integrationtest.py
```

Harness использует отдельный ephemeral MinIO на `127.0.0.1:9002`, создаёт уникальный bucket из теста и удаляет контейнеры и volumes в `finally`. Эти credentials и порт относятся только к тестам. Production bucket создаётся оператором; worker не создаёт и не удаляет bucket автоматически.
