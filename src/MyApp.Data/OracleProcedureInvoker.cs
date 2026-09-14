using MyApp.Contracts.Outbound;
using Oracle.ManagedDataAccess.Client;

namespace MyApp.Data;

/// <summary>
/// Абстракция вызова Oracle-процедуры с произвольным набором параметров.
/// Вынесена отдельно от OutboundPollingWorker, чтобы в юнит-тестах можно было
/// подставить фейковую реализацию вместо реального подключения к Oracle.
/// </summary>
public interface IOracleProcedureInvoker
{
    Task InvokeAsync(string procedureName, IReadOnlyList<OracleProcedureParameter> parameters, CancellationToken ct);
}

public sealed class OracleProcedureInvoker(OracleConnectionFactory connectionFactory) : IOracleProcedureInvoker
{
    public async Task InvokeAsync(string procedureName, IReadOnlyList<OracleProcedureParameter> parameters, CancellationToken ct)
    {
        await using var connection = await connectionFactory.OpenAsync(ct);
        await using var command = connection.CreateCommand();

        var paramNames = string.Join(", ", parameters.Select(p => $":{p.Name}"));
        command.CommandText = $"BEGIN {procedureName}({paramNames}); END;";

        foreach (var p in parameters)
        {
            command.Parameters.Add(new OracleParameter(p.Name, p.DbType) { Value = p.Value ?? DBNull.Value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
