# MyApp — интеграционный сервис Kafka ↔ Oracle

## Структура

```
src/
  MyApp.Host/               Generic Host (Worker), composition root, appsettings.json
  MyApp.Contracts/          Контракты событий: IInboundEventHandler<T>, IOutboundEventSource<T>
  MyApp.Core/                Generic-воркеры, DI-регистрация по конфигу, резолвинг по Code
  MyApp.Handlers/           Реализации IInboundEventHandler<T> (пример: OrderCreatedHandler)
  MyApp.OutboundSources/    Реализации IOutboundEventSource<T> (пример: InvoiceReadySource)
  MyApp.Data/                OracleConnectionFactory и общие Oracle-настройки
```

## Запуск

1. Установить .NET 8 SDK.
2. Поднять/указать Kafka и Oracle в `src/MyApp.Host/appsettings.json` (для
   прод-креденшлов — переменные окружения, см. раздел "Безопасность Kafka" ниже).
3. Из корня решения:

```bash
dotnet restore MyApp.sln
dotnet build MyApp.sln
dotnet run --project src/MyApp.Host
```

## Добавление нового события — чек-лист

### Входящее (Kafka → Oracle)

1. DTO сообщения — в `MyApp.Contracts` (или прямо в `MyApp.Handlers`, если контракт
   используется только внутри одного обработчика).
2. Класс, реализующий `IInboundEventHandler<TMessage>`, в `MyApp.Handlers`.
   Обязательно **идемпотентная** запись в БД (`MERGE`/upsert по бизнес-ключу) —
   offset коммитится только после успешной обработки, при сбое сообщение
   будет доставлено повторно.
3. Новый item в секции `InboundEvents` в `appsettings.json`:
   `Code`, `SourceQueue`, `ConsumerGroup`, `Handler` (полное имя типа).

### Исходящее (Oracle view → Kafka)

1. DTO сообщения — в `MyApp.Contracts` (или в `MyApp.OutboundSources`).
2. Класс, реализующий `IOutboundEventSource<TMessage>`, в `MyApp.OutboundSources`:
   - `FetchPendingAsync` — выборка "нового" из view;
   - `GetSuccessParameters` / `GetErrorParameters` — параметры для вызова
     Success-/Error-процедуры (сигнатуры процедур могут быть произвольными
     и независимыми друг от друга).
3. Новый item в секции `OutboundEvents` в `appsettings.json`:
   `Code`, `SourceView`, `SuccessProcedure`, `ErrorProcedure`, `TargetTopic`,
   `PollingIntervalSeconds`, `Source` (полное имя типа).

**Больше никаких правок в `MyApp.Host`/`MyApp.Core` не требуется.** DI на старте
либо свяжет всё автоматически по конфигу, либо упадёт с понятной ошибкой
("handler type not found", "must implement IInboundEventHandler<T>" и т.п.).

## Гарантии доставки

Используется **at-least-once + идемпотентность**, а не строгий exactly-once
(недостижим без распределённых транзакций между Kafka и Oracle):

- **Inbound**: offset коммитится только после успешного `HandleAsync`; повторная
  доставка того же сообщения покрывается `MERGE`-логикой в обработчике.
- **Outbound**: producer работает с `EnableIdempotence = true`; при сбое между
  отправкой в Kafka и вызовом Success/Error-процедуры строка остаётся во view
  и будет переотправлена на следующем цикле polling'а — получатели должны
  делать upsert по `IdempotencyKey` (= ключу Kafka-сообщения).

DLQ не используется — ошибка просто не подтверждается, элемент переигрывается
на следующей итерации.

## Безопасность Kafka (SASL_SSL)

Настройки безопасности (`Kafka:Security` в конфиге) применяются **одним и тем
же кодом** (`KafkaSecurityConfigurator.ApplySecurity`) и к продюсеру, и ко всем
consumer-воркерам — гарантированно не могут разойтись между ними, потому что
`ConsumerConfig` и `ProducerConfig` оба наследуются от `ClientConfig`, на
котором и объявлено расширение.

```jsonc
"Kafka": {
  "BootstrapServers": "kafka-broker-1:9093,kafka-broker-2:9093",
  "Security": {
    "SecurityProtocol": "SaslSsl",
    "SaslMechanism": "ScramSha512",
    "SaslUsername": "myapp-service",
    "SaslPassword": "...",
    "SslCaLocation": "/etc/myapp/certs/ca.pem",
    "EnableSslCertificateVerification": true
  }
}
```

Если секцию `Security` не указывать (как в базовом `appsettings.json`) —
подключение идёт без аутентификации/шифрования, что подходит для локального
dev-брокера. Полный пример боевого конфига — в
`src/MyApp.Host/appsettings.Production.json.example` (расширение `.example`,
чтобы ASP.NET Core не подхватывал файл автоматически как реальный конфиг).

**Пароль (`SaslPassword`) и пароль от ключа (`SslKeyPassword`) никогда не
хранятся в `appsettings.*.json` в git.** Оставляйте их пустыми/отсутствующими
в файлах конфигурации и переопределяйте через переменные окружения — Generic
Host (`Host.CreateApplicationBuilder`) уже по умолчанию подключает провайдер
переменных окружения, дополнительно ничего включать не нужно:

```bash
# двойное подчёркивание = разделитель уровней секции конфига
export Kafka__Security__SaslPassword="..."
```

При деплое в Kubernetes это обычно означает: значение лежит в Vault, попадает
в Secret (напрямую или через Vault Agent/External Secrets Operator), и
секция `env`/`envFrom` в манифесте пода прокидывает его в переменную
`Kafka__Security__SaslPassword` — приложение получит его без каких-либо
изменений в коде.

При mTLS (клиентский сертификат) дополнительно заполните `SslCertificateLocation`
и `SslKeyLocation` (пути к PEM-файлам, смонтированным в контейнер/под).

Некорректное значение `SecurityProtocol`/`SaslMechanism` в конфиге валится на
старте хоста с понятной ошибкой (`Kafka:Security:SecurityProtocol - unknown
value '...'`), а не где-то в середине работы при первой попытке подключения.

## Health checks

Хост поднимает Kestrel (порт задаётся в `Kestrel:Endpoints:Http:Url`, по
умолчанию `8080`; можно переопределить через `ASPNETCORE_URLS`) с двумя
эндпоинтами:

| Эндпоинт | Что проверяет | Код ответа |
|---|---|---|
| `GET /health/live` | Только то, что процесс жив и отвечает на HTTP. **Не** проверяет Oracle/Kafka — сбой внешней системы не повод убивать и перезапускать под, это не решит проблему, а вызовет рестарт-шторм. | всегда 200, если процесс жив |
| `GET /health/ready` | Все проверки с тегом `ready`: подключение к Oracle, доступность Kafka-брокеров (через существующий продюсер), и по одной проверке на **каждое** зарегистрированное inbound/outbound событие. | 200 (`Healthy`) / 503 (`Degraded` или `Unhealthy`) |

`/health/ready` отдаёт JSON с разбивкой по каждой проверке:

```json
{
  "status": "Unhealthy",
  "totalDurationMs": 12.4,
  "checks": [
    { "name": "oracle", "status": "Healthy", "description": "Oracle connection OK", "durationMs": 8.1 },
    { "name": "kafka-producer", "status": "Healthy", "description": "Kafka reachable, 3 broker(s)", "durationMs": 3.2 },
    { "name": "inbound:OrderCreated", "status": "Healthy", "description": "OrderCreated: OK", "durationMs": 0.0 },
    { "name": "outbound:InvoiceReady", "status": "Unhealthy", "description": "InvoiceReady: Oracle connection failed", "durationMs": 0.1 }
  ]
}
```

Как считается здоровье конкретного события (`inbound:<Code>` / `outbound:<Code>`):

- Каждый воркер (`InboundConsumerWorker`/`OutboundPollingWorker`) сам репортит
  результат своей работы в общий `WorkerHealthState` — `ReportSuccess()` после
  удачной обработки/poll-цикла, `ReportFailure(ex.Message)` при исключении.
  Health check только читает это состояние, не делает никаких сетевых вызовов
  сам.
- **Inbound**: если consumer ни разу не упал — `Healthy`, независимо от того,
  сколько времени не было сообщений (это легитимная ситуация, не "нездоровье").
- **Outbound**: дополнительно проверяется staleness — poll-цикл должен успешно
  отрабатывать (даже с 0 найденных строк) не реже, чем раз в
  `max(3 × PollingIntervalSeconds, PollingIntervalSeconds + 30)` секунд, иначе
  `Degraded`. Единичный сбой конкретной строки (Success/Error-процедура не
  вызвалась) на здоровье воркера **не влияет** — это штатная ситуация at-least-once,
  а не потеря соединения.

Разработчику нового события никакого дополнительного кода для health checks
писать не нужно — они регистрируются автоматически внутри
`AddInboundEvents`/`AddOutboundEvents` на каждый `Code` из конфига.

Пример проб в манифесте Kubernetes:

```yaml
livenessProbe:
  httpGet: { path: /health/live, port: 8080 }
  initialDelaySeconds: 10
  periodSeconds: 15
readinessProbe:
  httpGet: { path: /health/ready, port: 8080 }
  initialDelaySeconds: 5
  periodSeconds: 10
```

## Тесты

`tests/MyApp.Tests` — юнит-тесты на xUnit + Moq. Покрывают:

- `InboundHandlerAdapter` — десериализация JSON, делегирование в обработчик,
  распространение исключений.
- `AddInboundEvents` / `AddOutboundEvents` — fail-fast при отсутствующем/неверном
  типе в конфиге, корректный wiring через keyed DI.
- `InboundConsumerWorker.ProcessMessageAsync` — commit offset только при успехе,
  отсутствие commit при исключении обработчика.
- `OutboundSourceAdapter` — кеширование выборки по `IdempotencyKey`, очистка кеша
  на новом цикле.
- `OutboundPollingWorker.ProcessItemAsync` — вызов Success/Error-процедуры в
  зависимости от результата `Produce` (успех / `NotPersisted` / исключение),
  и то, что сбой самого вызова процедуры не приводит к падению воркера.
- `KafkaSecurityConfigurator.ApplySecurity` — корректное применение SASL_SSL-
  настроек и на `ConsumerConfig`, и на `ProducerConfig`; fail-fast при неверном
  `SecurityProtocol`/`SaslMechanism`.
- `WorkerHealthState` / `WorkerHealthEvaluator` — переходы Healthy ↔ Unhealthy,
  приоритет явной ошибки над staleness, поведение с/без порога устаревания.

Запуск:

```bash
dotnet test MyApp.sln
```

**Важно:** для тестируемости в `MyApp.Core`/`MyApp.Data` выделены две абстракции,
которых нет в "боевом" пути напрямую:

- `IKafkaConsumerFactory` — создание `IConsumer<string,string>` (реальная
  реализация — `KafkaConsumerFactory`), позволяет подставлять фейковый consumer
  в тестах вместо реального подключения к брокеру.
- `IOracleProcedureInvoker` — вызов Oracle-процедуры с параметрами (реальная
  реализация — `OracleProcedureInvoker` в `MyApp.Data`), позволяет не поднимать
  реальную БД в юнит-тестах.

Обе регистрируются в `Program.cs` как singleton и не меняют поведение в проде —
это чистое разделение ответственности для инверсии зависимостей.

Тесты, требующие реального Oracle (например, `MERGE`-логику в
`OrderCreatedHandler` или SQL в `InvoiceReadySource`), рекомендуется писать как
**интеграционные**, а не юнит-тесты — например, через Testcontainers с образом
Oracle XE, отдельным проектом `MyApp.IntegrationTests`. Юнит-тесты на них не
имеют смысла без реальной СУБД: единственная логика, которую стоит проверять
изолированно, уже вынесена в `IOracleProcedureInvoker`.
