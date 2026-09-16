using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using MyApp.Contracts.Outbound;
using MyApp.Core.Configuration;
using MyApp.Core.Health;

namespace MyApp.Core.Outbound;

public sealed record OutboundEventRegistration(
    string Code,
    string SourceView,
    string TargetTopic,
    string SuccessProcedure,
    string ErrorProcedure,
    int PollingIntervalSeconds);

public sealed record RawOutboundItem(string IdempotencyKey, string PayloadJson);

/// <summary>
/// Type-erased фасад над IOutboundEventSource&lt;TMessage&gt;. Кеширует последнюю выборку
/// по IdempotencyKey, чтобы воркер мог запрашивать Success/Error-параметры,
/// не зная о TMessage.
/// </summary>
public interface IOutboundEventSourceBase
{
    Task<IReadOnlyList<RawOutboundItem>> FetchPendingRawAsync(CancellationToken ct);

    IReadOnlyList<OracleProcedureParameter> GetSuccessParameters(string idempotencyKey);

    IReadOnlyList<OracleProcedureParameter> GetErrorParameters(string idempotencyKey, ProcessingError error);
}

internal sealed class OutboundSourceAdapter<TMessage>(
    IOutboundEventSource<TMessage> inner,
    JsonSerializerOptions jsonOptions) : IOutboundEventSourceBase
{
    private readonly Dictionary<string, OutboundItem<TMessage>> _cache = new();

    public async Task<IReadOnlyList<RawOutboundItem>> FetchPendingRawAsync(CancellationToken ct)
    {
        var items = await inner.FetchPendingAsync(ct);

        _cache.Clear();
        var result = new List<RawOutboundItem>(items.Count);
        foreach (var item in items)
        {
            _cache[item.IdempotencyKey] = item;
            result.Add(new RawOutboundItem(item.IdempotencyKey, JsonSerializer.Serialize(item.Payload, jsonOptions)));
        }

        return result;
    }

    public IReadOnlyList<OracleProcedureParameter> GetSuccessParameters(string idempotencyKey)
        => inner.GetSuccessParameters(_cache[idempotencyKey]);

    public IReadOnlyList<OracleProcedureParameter> GetErrorParameters(string idempotencyKey, ProcessingError error)
        => inner.GetErrorParameters(_cache[idempotencyKey], error);
}

public static class OutboundEventRegistrationExtensions
{
    /// <summary>
    /// Регистрирует все outbound-события из конфига: DI-биндинг источника,
    /// typed-adapter и HostedService (OutboundPollingWorker) на каждый Code.
    /// Падает на старте с понятной ошибкой, если Source не найден или не реализует контракт.
    /// </summary>
    public static IServiceCollection AddOutboundEvents(
        this IServiceCollection services, IEnumerable<OutboundEventConfig> configs)
    {
        foreach (var cfg in configs)
        {
            var sourceType = Type.GetType(cfg.Source, throwOnError: false)
                ?? throw new InvalidOperationException(
                    $"OutboundEvents: Code='{cfg.Code}' - source type '{cfg.Source}' not found. " +
                    "Check assembly/namespace name in configuration.");

            var sourceInterface = sourceType.GetInterfaces().FirstOrDefault(i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IOutboundEventSource<>))
                ?? throw new InvalidOperationException(
                    $"OutboundEvents: Code='{cfg.Code}' - type '{sourceType.FullName}' must implement " +
                    $"IOutboundEventSource<T>.");

            var messageType = sourceInterface.GenericTypeArguments[0];

            services.AddScoped(sourceType);
            services.AddScoped(sourceInterface, sourceType);

            var adapterType = typeof(OutboundSourceAdapter<>).MakeGenericType(messageType);
            services.AddKeyedScoped(typeof(IOutboundEventSourceBase), cfg.Code,
                (sp, _) => ActivatorUtilities.CreateInstance(sp, adapterType));

            var registration = new OutboundEventRegistration(
                cfg.Code, cfg.SourceView, cfg.TargetTopic, cfg.SuccessProcedure, cfg.ErrorProcedure,
                cfg.PollingIntervalSeconds);
            services.AddSingleton(registration);

            var healthState = new WorkerHealthState();

            services.AddHostedService(sp =>
                ActivatorUtilities.CreateInstance<OutboundPollingWorker>(sp, registration, healthState));

            // staleAfter: poll-цикл должен успешно отрабатывать (даже с 0 найденных строк)
            // не реже, чем раз в 3 интервала поллинга - иначе это уже не "тихо", а "застряло".
            var staleAfter = TimeSpan.FromSeconds(Math.Max(cfg.PollingIntervalSeconds * 3, cfg.PollingIntervalSeconds + 30));
            services.AddHealthChecks().AddAsyncCheck(
                $"outbound:{cfg.Code}",
                _ => Task.FromResult(WorkerHealthEvaluator.Evaluate(healthState, cfg.Code, staleAfter)),
                tags: ["ready"]);
        }

        return services;
    }
}
