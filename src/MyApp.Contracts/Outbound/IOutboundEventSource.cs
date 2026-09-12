using Oracle.ManagedDataAccess.Client;

namespace MyApp.Contracts.Outbound;

/// <summary>
/// Контракт источника исходящего события одного типа.
/// FetchPendingAsync читает "новое" из view.
/// GetSuccessParameters/GetErrorParameters формируют параметры для вызова
/// Success- или Error-процедуры (имена процедур задаются в конфиге,
/// сами процедуры могут иметь произвольные и полностью независимые сигнатуры).
/// </summary>
public interface IOutboundEventSource<TMessage>
{
    Task<IReadOnlyList<OutboundItem<TMessage>>> FetchPendingAsync(CancellationToken ct);

    IReadOnlyList<OracleProcedureParameter> GetSuccessParameters(OutboundItem<TMessage> item);

    IReadOnlyList<OracleProcedureParameter> GetErrorParameters(OutboundItem<TMessage> item, ProcessingError error);
}

/// <param name="IdempotencyKey">
/// Бизнес-ключ записи (обычно PK строки во view). Используется как Kafka message key
/// и для сопоставления результата продюсирования с исходной записью.
/// </param>
public sealed record OutboundItem<TMessage>(string IdempotencyKey, TMessage Payload);

public sealed record OracleProcedureParameter(string Name, object? Value, OracleDbType DbType);

public sealed record ProcessingError(string Reason, Exception? Exception = null);

public abstract record ProcessingResult
{
    public sealed record Success(IReadOnlyList<OracleProcedureParameter> Parameters) : ProcessingResult;

    public sealed record Failure(IReadOnlyList<OracleProcedureParameter> Parameters) : ProcessingResult;
}
