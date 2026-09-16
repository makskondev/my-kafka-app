using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MyApp.Core.Kafka;

/// <summary>
/// Проверяет доступность брокеров через уже существующий singleton-продюсер
/// (не открывает отдельное соединение) - запрашивает метаданные кластера.
/// </summary>
public sealed class KafkaProducerHealthCheck(IProducer<string, string> producer) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var metadata = producer.GetMetadata(TimeSpan.FromSeconds(5));

            return Task.FromResult(metadata.Brokers.Count > 0
                ? HealthCheckResult.Healthy($"Kafka reachable, {metadata.Brokers.Count} broker(s)")
                : HealthCheckResult.Unhealthy("Kafka metadata returned no brokers"));
        }
        catch (Exception ex)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("Kafka broker unreachable", ex));
        }
    }
}
