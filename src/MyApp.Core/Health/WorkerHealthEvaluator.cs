using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MyApp.Core.Health;

/// <summary>
/// Превращает WorkerHealthState в HealthCheckResult. Вынесено в чистую статическую
/// функцию (без обращения к DI/HTTP), чтобы логику пороговых значений можно было
/// протестировать без поднятия HealthCheckService.
/// </summary>
public static class WorkerHealthEvaluator
{
    /// <param name="staleAfter">
    /// Для outbound-воркеров: если с последней УСПЕШНОЙ активности прошло больше этого
    /// времени - Degraded (poll-цикл должен запускаться регулярно, даже если ничего не
    /// нашёл во view). Для inbound-воркеров передавайте null - соединение может простаивать
    /// легитимно долго при отсутствии сообщений, поэтому staleness не применяется.
    /// </param>
    public static HealthCheckResult Evaluate(WorkerHealthState state, string code, TimeSpan? staleAfter)
    {
        if (!state.IsHealthy)
        {
            return HealthCheckResult.Unhealthy($"{code}: {state.LastError}");
        }

        if (staleAfter is { } threshold)
        {
            var age = DateTimeOffset.UtcNow - state.LastActivityUtc;
            if (age > threshold)
            {
                return HealthCheckResult.Degraded(
                    $"{code}: no successful activity for {age:g} (threshold {threshold:g})");
            }
        }

        return HealthCheckResult.Healthy($"{code}: OK");
    }
}
