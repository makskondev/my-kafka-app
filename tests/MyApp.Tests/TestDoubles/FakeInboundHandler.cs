using MyApp.Contracts.Inbound;

namespace MyApp.Tests.TestDoubles;

public sealed record FakeInboundMessage(int Value, string Text);

/// <summary>
/// Реализация без зависимости от Oracle - используется в тестах регистрации/wiring,
/// где важно только то, что тип корректно резолвится через DI по конфигу.
/// </summary>
public sealed class FakeInboundHandler : IInboundEventHandler<FakeInboundMessage>
{
    public Task HandleAsync(FakeInboundMessage message, InboundEventContext context, CancellationToken ct)
        => Task.CompletedTask;
}

/// <summary>Класс, который НЕ реализует IInboundEventHandler&lt;T&gt; - для проверки fail-fast.</summary>
public sealed class NotAnInboundHandler;
