using MyApp.Contracts.Outbound;

namespace MyApp.Tests.TestDoubles;

public sealed record FakeOutboundPayload(int Value);

/// <summary>
/// Реализация без зависимости от Oracle - используется в тестах регистрации/wiring.
/// </summary>
public sealed class FakeOutboundSource : IOutboundEventSource<FakeOutboundPayload>
{
    public Task<IReadOnlyList<OutboundItem<FakeOutboundPayload>>> FetchPendingAsync(CancellationToken ct)
        => Task.FromResult<IReadOnlyList<OutboundItem<FakeOutboundPayload>>>([]);

    public IReadOnlyList<OracleProcedureParameter> GetSuccessParameters(OutboundItem<FakeOutboundPayload> item)
        => [];

    public IReadOnlyList<OracleProcedureParameter> GetErrorParameters(
        OutboundItem<FakeOutboundPayload> item, ProcessingError error)
        => [];
}

/// <summary>Класс, который НЕ реализует IOutboundEventSource&lt;T&gt; - для проверки fail-fast.</summary>
public sealed class NotAnOutboundSource;
