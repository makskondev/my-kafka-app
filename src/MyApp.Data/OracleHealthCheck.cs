using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MyApp.Data;

/// <summary>Проверяет, что подключение к Oracle реально устанавливается и БД отвечает.</summary>
public sealed class OracleHealthCheck(OracleConnectionFactory connectionFactory) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await connectionFactory.OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1 FROM DUAL";
            await command.ExecuteScalarAsync(cancellationToken);

            return HealthCheckResult.Healthy("Oracle connection OK");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Oracle connection failed", ex);
        }
    }
}
