# Как добавить новое событие

Это шпаргалка для разработчиков, которые добавляют новый тип входящего или
исходящего события в сервис. Ядро (`MyApp.Core`, `MyApp.Host`) при этом
**не меняется** — только новые файлы + одна запись в конфиге.

---

## Входящее событие (Kafka → Oracle)

Пример ниже — на условном событии `PaymentReceived`.

### Шаг 1. DTO сообщения

Создайте record в `MyApp.Handlers` (или в `MyApp.Contracts`, если DTO
переиспользуется где-то ещё). Имена свойств должны совпадать с полями JSON
в очереди (десериализация регистронезависима, `PropertyNameCaseInsensitive = true`).

```csharp
// MyApp.Handlers/PaymentReceivedMessage.cs
namespace MyApp.Handlers;

public sealed record PaymentReceivedMessage(long PaymentId, long OrderId, decimal Amount, string Currency);
```

### Шаг 2. Обработчик

Реализуйте `IInboundEventHandler<TMessage>`. **Единственное жёсткое требование:
запись в БД должна быть идемпотентной** — используйте `MERGE`, а не `INSERT`.
Offset коммитится только после успешного завершения `HandleAsync`; при сбое
(упал под, оборвалось соединение с Oracle и т.п.) то же сообщение придёт снова.

```csharp
// MyApp.Handlers/PaymentReceivedHandler.cs
using MyApp.Contracts;
using MyApp.Contracts.Inbound;
using MyApp.Data;
using Oracle.ManagedDataAccess.Client;

namespace MyApp.Handlers;

[EventType("PaymentReceived")] // необязательно, но помогает ориентироваться в коде
public sealed class PaymentReceivedHandler(OracleConnectionFactory connectionFactory)
    : IInboundEventHandler<PaymentReceivedMessage>
{
    public async Task HandleAsync(PaymentReceivedMessage message, InboundEventContext context, CancellationToken ct)
    {
        await using var connection = await connectionFactory.OpenAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            MERGE INTO PAYMENTS p
            USING (SELECT :paymentId AS PAYMENT_ID FROM DUAL) src
            ON (p.PAYMENT_ID = src.PAYMENT_ID)
            WHEN NOT MATCHED THEN
              INSERT (PAYMENT_ID, ORDER_ID, AMOUNT, CURRENCY, CREATED_AT)
              VALUES (:paymentId, :orderId, :amount, :currency, SYSTIMESTAMP)
            """;

        command.Parameters.Add(new OracleParameter("paymentId", message.PaymentId));
        command.Parameters.Add(new OracleParameter("orderId", message.OrderId));
        command.Parameters.Add(new OracleParameter("amount", message.Amount));
        command.Parameters.Add(new OracleParameter("currency", message.Currency));

        await command.ExecuteNonQueryAsync(ct);
    }
}
```

> Если у события есть "статусный" смысл (не просто создание, а изменение), в
> `MERGE ... WHEN MATCHED THEN UPDATE` добавьте условие, что обновлять стоит
> только если новое состояние "новее" старого (например, по версии или
> timestamp) — иначе повторная доставка старого сообщения после более свежего
> может откатить данные назад.

### Шаг 3. Запись в конфиге

Добавьте элемент в `InboundEvents` в `src/MyApp.Host/appsettings.json`:

```jsonc
{
  "Code": "PaymentReceived",
  "SourceQueue": "payments.received.v1",
  "ConsumerGroup": "myapp-payment-received",
  "Handler": "MyApp.Handlers.PaymentReceivedHandler, MyApp.Handlers"
}
```

| Поле | Что это |
|---|---|
| `Code` | Уникальный идентификатор события. Используется для связи конфига и класса, ни на что другое не влияет. |
| `SourceQueue` | Имя Kafka-топика, который слушаем. |
| `ConsumerGroup` | Consumer group — должна быть уникальной на событие (иначе разные типы событий будут делить один offset-курсор). |
| `Handler` | `Namespace.ClassName, AssemblyName` — **точное** имя сборки (`MyApp.Handlers`, без `.dll`). |

### Готово

При старте хост создаст под это событие отдельный `BackgroundService`
(consumer group `myapp-payment-received`, топик `payments.received.v1`) и
свяжет его с `PaymentReceivedHandler`. Ничего в `MyApp.Core`/`MyApp.Host`
трогать не нужно.

---

## Исходящее событие (Oracle view → Kafka)

Пример ниже — на условном событии `ShipmentDispatched`.

### Шаг 1. DTO сообщения

```csharp
// MyApp.OutboundSources/ShipmentDispatchedMessage.cs
namespace MyApp.OutboundSources;

public sealed record ShipmentDispatchedMessage(long ShipmentId, long OrderId, string Carrier, string TrackingNumber);
```

### Шаг 2. Источник

Реализуйте `IOutboundEventSource<TMessage>` — три метода:

```csharp
// MyApp.OutboundSources/ShipmentDispatchedSource.cs
using MyApp.Contracts;
using MyApp.Contracts.Outbound;
using MyApp.Core.Outbound;
using MyApp.Data;
using Oracle.ManagedDataAccess.Client;
using System.Globalization;

namespace MyApp.OutboundSources;

[EventType("ShipmentDispatched")]
public sealed class ShipmentDispatchedSource(
    OracleConnectionFactory connectionFactory,
    OutboundEventRegistration registration) // сюда фреймворк подставит SourceView/Code из конфига
    : IOutboundEventSource<ShipmentDispatchedMessage>
{
    // 1. Выборка "нового" из view
    public async Task<IReadOnlyList<OutboundItem<ShipmentDispatchedMessage>>> FetchPendingAsync(CancellationToken ct)
    {
        var result = new List<OutboundItem<ShipmentDispatchedMessage>>();

        await using var connection = await connectionFactory.OpenAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT SHIPMENT_ID, ORDER_ID, CARRIER, TRACKING_NUMBER FROM {registration.SourceView}";

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var id = reader.GetInt64(0);
            result.Add(new OutboundItem<ShipmentDispatchedMessage>(
                IdempotencyKey: id.ToString(CultureInfo.InvariantCulture), // = Kafka message key
                Payload: new ShipmentDispatchedMessage(id, reader.GetInt64(1), reader.GetString(2), reader.GetString(3))));
        }

        return result;
    }

    // 2. Параметры для SuccessProcedure (вызывается, если Produce в Kafka прошёл)
    public IReadOnlyList<OracleProcedureParameter> GetSuccessParameters(OutboundItem<ShipmentDispatchedMessage> item) =>
    [
        new("shipmentId", item.Payload.ShipmentId, OracleDbType.Int64),
        new("sentAt", DateTime.UtcNow, OracleDbType.TimeStamp),
    ];

    // 3. Параметры для ErrorProcedure (вызывается при сбое Produce)
    public IReadOnlyList<OracleProcedureParameter> GetErrorParameters(
        OutboundItem<ShipmentDispatchedMessage> item, ProcessingError error) =>
    [
        new("shipmentId", item.Payload.ShipmentId, OracleDbType.Int64),
        new("errorMessage", Truncate(error.Reason, 4000), OracleDbType.Varchar2),
    ];

    private static string Truncate(string value, int maxLength) => value.Length > maxLength ? value[..maxLength] : value;
}
```

Ключевые моменты:

- **`IdempotencyKey`** — это одновременно и Kafka message key, и то, что
  передаётся в `Get*Parameters`. Берите его из первичного ключа строки во
  view — он должен однозначно определять запись.
- **`GetSuccessParameters`/`GetErrorParameters` могут иметь совершенно разные
  сигнатуры** — Success- и Error-процедуры в Oracle не обязаны совпадать по
  набору параметров. Параметр `Name` в `OracleProcedureParameter` должен
  совпадать с именем позиционного аргумента процедуры (порядок вызова строится
  по порядку элементов списка).
- Success-процедура вызывается **только если** `Produce` в Kafka подтвердил
  `PersistenceStatus.Persisted`. Если Success-процедура сама упадёт (например,
  Oracle моргнул на секунду) — строка просто остаётся во view и уйдёт в Kafka
  ещё раз на следующем poll'е. Получатель в топике должен уметь делать upsert
  по `IdempotencyKey`, чтобы повтор не был проблемой.

### Шаг 3. Запись в конфиге

```jsonc
{
  "Code": "ShipmentDispatched",
  "SourceView": "V_PENDING_SHIPMENTS",
  "SuccessProcedure": "PKG_SHIPMENT.MARK_SENT",
  "ErrorProcedure": "PKG_SHIPMENT.MARK_FAILED",
  "TargetTopic": "shipments.dispatched.v1",
  "PollingIntervalSeconds": 15,
  "Source": "MyApp.OutboundSources.ShipmentDispatchedSource, MyApp.OutboundSources"
}
```

| Поле | Что это |
|---|---|
| `Code` | Уникальный идентификатор события. |
| `SourceView` | Имя view с "новыми" записями. Прокидывается в источник через `OutboundEventRegistration.SourceView` — не хардкодьте имя в SQL. |
| `SuccessProcedure` / `ErrorProcedure` | `PACKAGE.PROCEDURE` (или просто `PROCEDURE`, если без пакета), вызываются как `BEGIN Proc(:p0, :p1, ...); END;`. Могут иметь произвольные и независимые друг от друга сигнатуры. |
| `TargetTopic` | Топик, в который публикуем. |
| `PollingIntervalSeconds` | Как часто опрашивать view. |
| `Source` | `Namespace.ClassName, AssemblyName`. |

Убедитесь, что **сама view исключает строку после вызова Success/Error-процедуры**
(процедура должна проставлять статус/флаг в базовой таблице, на которой
построена view) — иначе одна и та же запись будет уходить в Kafka на каждом
цикле poll'а бесконечно.

### Готово

---

## Чек-лист (коротко)

**Входящее:**
- [ ] DTO сообщения
- [ ] Класс `IInboundEventHandler<T>` с идемпотентным `MERGE`
- [ ] Item в `InboundEvents` конфига (`Code`, `SourceQueue`, `ConsumerGroup`, `Handler`)

**Исходящее:**
- [ ] DTO сообщения
- [ ] Класс `IOutboundEventSource<T>` (`FetchPendingAsync` + `GetSuccessParameters` + `GetErrorParameters`)
- [ ] View гарантированно перестаёт отдавать строку после Success/Error-процедуры
- [ ] Item в `OutboundEvents` конфига (`Code`, `SourceView`, `SuccessProcedure`, `ErrorProcedure`, `TargetTopic`, `Source`)

**В обоих случаях НЕ трогаем:** `MyApp.Core`, `MyApp.Host/Program.cs`.

---

## Если что-то пошло не так на старте

DI проверяет конфиг при старте хоста и валится с понятной ошибкой ещё до
подключения к Kafka/Oracle:

| Сообщение | Причина |
|---|---|
| `InboundEvents: Code='X' - handler type '...' not found` | Опечатка в `Handler`, либо неверно указано имя сборки после запятой (должно быть имя `.csproj`/assembly, а не namespace). |
| `InboundEvents: Code='X' - type '...' must implement IInboundEventHandler<T>` | Класс существует, но не реализует нужный интерфейс (или реализует не тот generic-параметр). |
| Аналогичные для `OutboundEvents: ... source type ...` | То же самое для `IOutboundEventSource<T>`. |
| `Duplicate keyed service registration` (условно) | Два события в конфиге используют одинаковый `Code`. `Code` должен быть уникален в пределах своего списка (`InboundEvents`/`OutboundEvents`). |

Если сборка с новым обработчиком не подключена как `ProjectReference` в
`MyApp.Host.csproj` — `Type.GetType(...)` не найдёт тип, даже если код
скомпилирован в другом проекте решения. `MyApp.Handlers` и
`MyApp.OutboundSources` уже подключены в `MyApp.Host.csproj` по умолчанию —
если вы создаёте *новый* проект под события, не забудьте добавить на него
`ProjectReference`.
