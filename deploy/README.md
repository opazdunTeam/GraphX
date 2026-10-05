# Deployment

Deployment не является таргетом аналитического MVP.

- локальная разработка будет использовать Docker Compose после появления запускаемых приложений;
- reverse proxy нужен только для необязательной внешней демонстрации;
- существующий `graphx-landing.service` относится к статическому лендингу и не задаёт архитектуру GraphX API/Web;
- production Compose и blue-green добавляются только при наличии времени после основного сценария.

См. [развёртывание и эксплуатацию](../docs/technical/12-deployment-and-operations.md).
