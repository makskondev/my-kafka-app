namespace MyApp.Core.Health;

/// <summary>
/// Потокобезопасное состояние "здоровья" одного воркера (inbound- или outbound-события).
/// Воркер сам вызывает ReportSuccess/ReportFailure после каждой попытки работы с внешней
/// системой (Kafka/Oracle); health check только читает это состояние, не выполняя
/// никаких обращений к внешним системам напрямую.
/// </summary>
public sealed class WorkerHealthState
{
    private volatile bool _isHealthy = true; // оптимистичный старт: до первой попытки нет причин считать unhealthy
    private volatile string? _lastError;
    private long _lastActivityUtcTicks = DateTimeOffset.UtcNow.UtcTicks;

    public bool IsHealthy => _isHealthy;

    public string? LastError => _lastError;

    public DateTimeOffset LastActivityUtc => new(_lastActivityUtcTicks, TimeSpan.Zero);

    public void ReportSuccess()
    {
        _isHealthy = true;
        _lastError = null;
        _lastActivityUtcTicks = DateTimeOffset.UtcNow.UtcTicks;
    }

    public void ReportFailure(string error)
    {
        _isHealthy = false;
        _lastError = error;
        _lastActivityUtcTicks = DateTimeOffset.UtcNow.UtcTicks;
    }
}
