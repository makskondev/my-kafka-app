using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using MyApp.Contracts.Inbound;
using MyApp.Core.Configuration;

namespace MyApp.Core.Inbound;

public sealed record InboundEventRegistration(string Code, string SourceQueue, string ConsumerGroup);

/// <summary>
/// Type-erased фасад над IInboundEventHandler&lt;TMessage&gt; для диспетчеризации по строковому Code
/// без reflection на каждое сообщение (JSON десериализуется в конкретный TMessage внутри адаптера).
/// </summary>
public interface IInboundEventHandlerBase
{
    Task HandleAsync(string rawJson, InboundEventContext context, CancellationToken ct);
}

internal sealed class InboundHandlerAdapter<TMessage>(
    IInboundEventHandler<TMessage> inner,
    JsonSerializerOptions jsonOptions) : IInboundEventHandlerBase
{
    public async Task HandleAsync(string rawJson, InboundEventContext context, CancellationToken ct)
    {
        var message = JsonSerializer.Deserialize<TMessage>(rawJson, jsonOptions)
            ?? throw new InvalidOperationException(
                $"Deserialize failed for EventType={context.EventTypeCode}, payload could not be mapped to {typeof(TMessage).Name}");

        await inner.HandleAsync(message, context, ct);
    }
}

public static class InboundEventRegistrationExtensions
{
    /// <summary>
    /// Регистрирует все inbound-события из конфига: DI-биндинг обработчика,
    /// typed-adapter и HostedService (InboundConsumerWorker) на каждый Code.
    /// Падает на старте с понятной ошибкой, если Handler не найден или не реализует контракт.
    /// </summary>
    public static IServiceCollection AddInboundEvents(
        this IServiceCollection services, IEnumerable<InboundEventConfig> configs)
    {
        foreach (var cfg in configs)
        {
            var handlerType = Type.GetType(cfg.Handler, throwOnError: false)
                ?? throw new InvalidOperationException(
                    $"InboundEvents: Code='{cfg.Code}' - handler type '{cfg.Handler}' not found. " +
                    "Check assembly/namespace name in configuration.");

            var handlerInterface = handlerType.GetInterfaces().FirstOrDefault(i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IInboundEventHandler<>))
                ?? throw new InvalidOperationException(
                    $"InboundEvents: Code='{cfg.Code}' - type '{handlerType.FullName}' must implement " +
                    $"IInboundEventHandler<T>.");

            var messageType = handlerInterface.GenericTypeArguments[0];

            services.AddScoped(handlerType);
            services.AddScoped(handlerInterface, handlerType);

            var adapterType = typeof(InboundHandlerAdapter<>).MakeGenericType(messageType);
            services.AddKeyedScoped(typeof(IInboundEventHandlerBase), cfg.Code,
                (sp, _) => ActivatorUtilities.CreateInstance(sp, adapterType));

            var registration = new InboundEventRegistration(cfg.Code, cfg.SourceQueue, cfg.ConsumerGroup);
            services.AddSingleton(registration);

            services.AddHostedService(sp =>
                ActivatorUtilities.CreateInstance<InboundConsumerWorker>(sp, registration));
        }

        return services;
    }
}
