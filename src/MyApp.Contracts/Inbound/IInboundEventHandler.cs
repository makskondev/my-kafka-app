namespace MyApp.Contracts.Inbound;

/// <summary>
/// Контракт обработчика входящего события одного типа.
/// Реализация ДОЛЖНА быть идемпотентной (MERGE/upsert по бизнес-ключу):
/// offset коммитится только после успешного завершения HandleAsync,
/// поэтому при сбое сообщение будет доставлено повторно.
/// </summary>
public interface IInboundEventHandler<TMessage>
{
    Task HandleAsync(TMessage message, InboundEventContext context, CancellationToken ct);
}

public sealed record InboundEventContext(string EventTypeCode, string Topic, string? Key);
