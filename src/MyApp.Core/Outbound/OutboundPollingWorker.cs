using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MyApp.Contracts.Outbound;
using MyApp.Core.Health;
using MyApp.Data;

namespace MyApp.Core.Outbound;

/// <summary>
/// Единый generic-воркер на ВСЕ outbound-события. Новый экземпляр создаётся
/// на каждый Code при регистрации (см. AddOutboundEvents) - разработчик источника
/// этот класс не пишет и не видит.
/// </summary>
public sealed class OutboundPollingWorker(
    OutboundEventRegistration registration,
    WorkerHealthState healthState,
    IServiceScopeFactory scopeFactory,
    IProducer<string, string> producer,
    IOracleProcedureInvoker procedureInvoker,
    ILogger<OutboundPollingWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(registration.PollingIntervalSeconds));

        logger.LogInformation("Outbound worker started. Code={Code}, View={View}, Topic={Topic}",
            registration.Code, registration.SourceView, registration.TargetTopic);

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await PollOnceAsync(stoppingToken);
                // цикл поллинга сам по себе отработал (даже если items.Count == 0) -
                // это и есть сигнал "воркер жив и может достучаться до Oracle/view".
                healthState.ReportSuccess();
            }
            catch (Exception ex)
            {
                healthState.ReportFailure(ex.Message);
                logger.LogError(ex, "Outbound polling cycle failed. Code={Code}", registration.Code);
            }
        }
    }

    /// <summary>
    /// Один цикл поллинга: читает "новое" из источника и обрабатывает каждый элемент.
    /// Выделено в internal метод, чтобы юнит-тесты могли запускать один цикл без PeriodicTimer.
    /// </summary>
    internal async Task PollOnceAsync(CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var source = scope.ServiceProvider
            .GetRequiredKeyedService<IOutboundEventSourceBase>(registration.Code);

        var items = await source.FetchPendingRawAsync(ct);
        if (items.Count == 0)
        {
            return;
        }

        foreach (var item in items)
        {
            await ProcessItemAsync(source, item, ct);
        }
    }

    internal async Task ProcessItemAsync(IOutboundEventSourceBase source, RawOutboundItem item, CancellationToken ct)
    {
        var result = await TryProduceAsync(source, item, ct);

        var (procedureName, parameters) = result switch
        {
            ProcessingResult.Success s => (registration.SuccessProcedure, s.Parameters),
            ProcessingResult.Failure f => (registration.ErrorProcedure, f.Parameters),
            _ => throw new InvalidOperationException("Unreachable ProcessingResult case."),
        };

        try
        {
            await procedureInvoker.InvokeAsync(procedureName, parameters, ct);
        }
        catch (Exception ackEx)
        {
            // строка останется во view и переобработается на следующем poll (at-least-once).
            // Это НЕ считается сбоем самого цикла поллинга (см. ExecuteAsync) - единичная
            // строка, а не потеря соединения с БД/Kafka.
            logger.LogError(ackEx, "Failed to call {Procedure}. Code={Code}, Key={Key}",
                procedureName, registration.Code, item.IdempotencyKey);
        }
    }

    private async Task<ProcessingResult> TryProduceAsync(
        IOutboundEventSourceBase source, RawOutboundItem item, CancellationToken ct)
    {
        try
        {
            var delivery = await producer.ProduceAsync(registration.TargetTopic,
                new Message<string, string> { Key = item.IdempotencyKey, Value = item.PayloadJson },
                ct);

            if (delivery.Status != PersistenceStatus.Persisted)
            {
                var error = new ProcessingError($"Kafka persistence status: {delivery.Status}");
                return new ProcessingResult.Failure(source.GetErrorParameters(item.IdempotencyKey, error));
            }

            return new ProcessingResult.Success(source.GetSuccessParameters(item.IdempotencyKey));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Produce threw. Code={Code}, Key={Key}", registration.Code, item.IdempotencyKey);
            var error = new ProcessingError("Produce threw an exception", ex);
            return new ProcessingResult.Failure(source.GetErrorParameters(item.IdempotencyKey, error));
        }
    }
}
