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
2. Поднять/указать Kafka и Oracle в `src/MyApp.Host/appsettings.json` (или через
   `dotnet user-secrets` / переменные окружения для продакшн-креденшлов).
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
