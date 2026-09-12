using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;

namespace MyApp.Data;

public sealed class OracleOptions
{
    public required string ConnectionString { get; init; }
}

public sealed class OracleConnectionFactory(IOptions<OracleOptions> options)
{
    public async Task<OracleConnection> OpenAsync(CancellationToken ct)
    {
        var connection = new OracleConnection(options.Value.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }
}
