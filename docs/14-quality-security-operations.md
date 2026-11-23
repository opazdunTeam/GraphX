# Качество, безопасность и эксплуатация

## Проверки по мере разработки

GraphX - учебный MVP, поэтому не каждая правка требует большого набора тестов. Однако ключевые границы должны проверяться с самого начала:

- SQL-миграции поднимаются на чистой базе;
- OpenAPI и JSON Schema проходят проверку;
- общие JSON fixtures одинаково проходят strict I-JSON, Schema и semantic validation в Go и Python;
- adapters разбирают fixtures без сети;
- consumer повторно получает то же сообщение без двойного эффекта;
- provenance ведёт от связи до artifact;
- ошибочное сопоставление можно отменить;
- временные интервалы и снимки не смешиваются.

Каждый обычный pull request запускает обязательный GitHub Actions workflow без path-фильтров. Независимые jobs `Go`, `Python`, `Web` и `PostgreSQL` выполняются параллельно и не требуют credentials внешних источников. Они проверяют форматирование, lint, typecheck или build, unit tests, OpenAPI и JSON Schema, актуальность generated-кода и миграции на чистой PostgreSQL 18. Live source smoke и deployment в этот workflow не входят и не блокируют pull request.

CI требует зафиксированные `go.sum`, `uv.lock` и `pnpm-lock.yaml` до восстановления dependency cache. Go запускается с `-mod=readonly`, Python синхронизируется через `uv sync --locked`, а web - через `pnpm install --frozen-lockfile`. Кеш ускоряет загрузку зависимостей, но не заменяет lock-файл и не содержит готовое рабочее дерево. Внешние GitHub Actions закреплены полными commit SHA, права workflow ограничены чтением содержимого репозитория.

Локальные эквиваленты jobs описаны в [правилах разработки](15-development.md#обязательный-ci). В `develop` разрешён прямой push, после которого полный CI запускается для раннего обнаружения ошибок. `main` принимает изменения только через Pull Request; checks `Go`, `Python`, `Web` и `PostgreSQL` обязательны перед merge через GitHub ruleset.

## Наблюдаемость

Go HTTP runtime пишет структурированные логи в настроенном формате `json` или `text` с `service` и `request_id`, а access log также содержит метод, шаблон маршрута, HTTP status и длительность. Логи предметных операций дополняются `investigation_id`, `job_id`, source и message id; `correlation_id` используется на messaging-границе. Секреты, полные документы, panic value и лишние персональные данные в лог не попадают.

Worker и adapter логи содержат только скалярные operational metadata из явного allowlist; остальные context-поля отбрасываются, а structured values закрываются целиком. Raw payload, HTTP headers/body, credentials, raw URL/URI, полные документы и лишние персональные данные запрещено передавать как сообщение или structured context; исходный ответ сохраняется как artifact. Artifact reference содержит только канонический HTTP(S) `source_uri` без userinfo, query и fragment и HTTP metadata из безопасного allowlist. Formatter дополнительно редактирует известные чувствительные поля и ограничивает размер значений и события, но это является защитой границы, а не разрешением логировать исходные данные.

Минимальные метрики:

- число jobs по статусам и источникам;
- длительность и доля ошибок источников;
- размер очередей и возраст старейшего сообщения;
- повторы и DLQ;
- необработанный outbox;
- число reviews и доля ручных решений;
- доступность PostgreSQL, RabbitMQ и MinIO.

Health endpoints приложения разделены на liveness и readiness. Сейчас readiness Go API подтверждает доступ к обязательной PostgreSQL с отдельным коротким timeout. RabbitMQ и MinIO должны добавляться в readiness только тогда, когда станут обязательными runtime-зависимостями соответствующего процесса; проверка не делает сетевой запрос во все внешние источники.

## Состояние внешних источников

Источники стоит отслеживать, но осторожно. Пассивное состояние строится по реальным заданиям: время последнего успеха, последовательность ошибок, rate limit и изменение формата. Это полезнее частого искусственного ping.

Активный health check допустим только для дешёвого и разрешённого запроса. Он запускается редко, имеет небольшой timeout и не используется для источников, где может вызвать блокировку, CAPTCHA или расход квоты. Состояния: `healthy`, `degraded`, `unavailable`, `action_required`, `unknown`.

Плохой health источника не блокирует всё расследование и не является единственным основанием отклонить job: это подсказка для планировщика и интерфейса.

## Минимальная безопасность

До локальной демонстрации обязательно:

- не публиковать PostgreSQL, RabbitMQ и MinIO в интернет;
- хранить секреты вне Git;
- ограничить размеры ответов, документов и архивов;
- проверять MIME type и безопасно обрабатывать имена файлов;
- запретить workers обращаться к loopback, link-local и внутренним адресам по данным из недоверенного ответа;
- поставить timeouts, лимиты редиректов и загрузок;
- не исполнять содержимое документов;
- очищать логи от токенов и чувствительных данных.

Встроенная регистрация для учебного MVP не нужна. Если демо выйдет за пределы локальной сети, минимальная защита - HTTPS и один внешний слой доступа: VPN, IP allowlist либо простой ключ/Basic Auth на Nginx. Это не заменяет полноценную авторизацию будущей платформы.

Политика хранения evidence пока не блокирует разработку, но до работы с реальными чувствительными материалами нужно определить срок хранения и способ удаления artifacts, backups и производных данных. Технический terminal replay journal имеет отдельное operational-окно: 30 суток по умолчанию, минимум 24 часа, quota по entries/bytes и очистку только свободных claims за пределами окна. Retention ограничивает гарантию exact replay, но не используется как срок допустимости входной команды. Окно должно быть не короче максимального периода возможной broker redelivery; при изменении retry/DLQ policy в #54 значения пересматриваются вместе.

## Конфигурация

Переменные перечислены в корневом `.env.example`. В репозиторий не добавляются настоящие пароли и токены. Основные группы:

- общие настройки и логирование;
- Go API;
- PostgreSQL;
- RabbitMQ;
- S3/MinIO;
- лимиты расследования;
- workers;
- флаги и credentials источников.

Go-процессы получают настройки через окружение и не загружают `.env` самостоятельно. `LOG_FORMAT` принимает только `json` или `text`. Для API обязателен `POSTGRES_DSN`; адрес HTTP, timeout и параметры пула имеют проверяемые значения по умолчанию.

`INVESTIGATION_DEFAULT_DEPTH` и `INVESTIGATION_DEFAULT_ENTITY_LIMIT` задают значения для отсутствующих request fields. Соответствующие `INVESTIGATION_MAX_*` ограничивают один deployment, но не могут превышать границы публичного OpenAPI. Каждый default должен находиться в допустимом диапазоне и не превышать свой deployment-максимум.

`VITE_API_BASE_URL` является build/dev-server настройкой web. При её отсутствии клиент использует относительный `/api/v1`; секреты нельзя передавать через переменные с префиксом `VITE_`, поскольку они попадают в клиентский bundle.

Настройки, меняющие бизнес-смысл данных, должны версионироваться вместе с алгоритмом, а не жить только в окружении.

## MinIO

Изолированный MinIO integration harness находится в `workers/docker-compose.test.yaml`; production Compose по-прежнему не добавляется до появления полного вертикального среза. Worker вычисляет SHA-256 в bounded spool, выполняет только conditional PUT с `If-None-Match: *` и никогда не заменяет существующий key. Совпадение после concurrent/uncertain PUT и recovery подтверждается потоковым пересчётом фактического тела; пользовательская S3 metadata не считается доказательством целостности. Видимость результата 409 ожидается с bounded exponential backoff в пределах `S3_RECONCILIATION_TIMEOUT`. PUT выполняется daemon-операцией, которая владеет spool до прекращения чтения. После отмены runtime ждёт регистрацию только в bounded drain-окне; повторная отмена или истечение окна немедленно дают uncertain infrastructure outcome. Delivery возвращается в очередь без terminal receipt, а graceful shutdown не обязан ждать зависший SDK-вызов. `S3_ALLOW_OVERWRITE=true` считается ошибкой конфигурации, а не рабочим режимом.

Неоднозначные PUT-ответы (`409`, `412`, retryable `5xx`, timeout и throttling) проходят один bounded reconciliation path. Reconciliation повторяет только временный 404, transport failure и retryable service error; `AccessDenied` и другие definitive HEAD errors сохраняют fatal-классификацию. Остальные service/configuration errors являются fatal runtime errors: команда возвращается в очередь, readiness снимается и consumer останавливается. Подтверждённая infrastructure failure имеет приоритет над одновременной cancellation/deadline. GET, streaming read и close принадлежат одному daemon worker с bounded chunk queue. Общий лимит `S3_MAX_INFLIGHT_OPERATIONS` учитывает операции до фактического завершения blocking SDK lifecycle, при настроенном storage не может быть ниже `WORKER_CONCURRENCY`; его исчерпание также останавливает consumer и не допускает накопления detached потоков и spool-файлов.

Локально versioning необязателен. При `S3_REQUIRE_VERSIONING=true` startup/readiness принимает только bucket со статусом `Enabled`; versioning не заменяет conditional PUT. Незавершённые multipart uploads должны иметь bucket lifecycle cleanup, а сроки удаления зарегистрированных evidence objects задаются отдельной retention policy до работы с реальными чувствительными материалами.

Тестовый образ MinIO закреплён release tag и digest. Команды integration harness описаны в `deploy/README.md` и `workers/README.md`.

## Развёртывание

Развёртывание - заключительная дополнительная работа, а не условие первого среза. Для одного VPS достаточно Docker Compose и Nginx. Kubernetes и отдельная платформа оркестрации не нужны.

Blue-green разумен только для stateless API и web. PostgreSQL, RabbitMQ и MinIO не дублируются таким способом. Миграции выполняются отдельно, до переключения трафика, и должны быть обратно совместимы хотя бы на время обновления.

До внешней демонстрации нужны резервная копия PostgreSQL, проверка восстановления и отдельная копия важных MinIO objects. Сам факт наличия backup без проверки restore недостаточен.
